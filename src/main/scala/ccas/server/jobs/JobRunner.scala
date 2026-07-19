package ccas.server.jobs

import java.nio.file.{Files, Path, Paths}
import java.sql.SQLException
import java.time.temporal.ChronoUnit

import com.typesafe.config.ConfigFactory

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.json.EncoderOps
import zio.stream.{SubscriptionRef, ZStream}
import zio.{Clock, Duration, Fiber, Promise, RIO, RLayer, Ref, Schedule, Scope, UIO, ZIO, ZLayer, durationInt}

import ccas.analysis.tables.{AppSetting, Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{ConflictException, safeMessage}
import ccas.utils.{BarSnapshot, JobLogSink, ProgressDisplay, ProgressSnapshot}

/** Asynchronous job executor that runs analysis tasks as forked fibers.
  *
  * Each submitted job is tracked in the `job_run` database table with a ULID identifier. Only one job of a given kind
  * (optionally scoped to a club) may run at a time; duplicate submissions are rejected with a [[ccas.utils.errors.ConflictException]].
  */
trait JobRunner {

  /** Fork a new job and return its ID. Fails with [[ccas.utils.errors.ConflictException]] if a matching job is already running.
    *
    * The effect receives the job run ID (as a string) so that analysis apps can link their own run records back to the
    * server-level job.
    */
  def submit(
    kind: JobKind,
    clubId: Option[ClubId],
    params: Option[String],
    trigger: RunTrigger,
    effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
  ): RIO[PostgresClient, JobRunId]

  /** Look up a job by ID, returning `None` if no such job exists. */
  def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]]

  /** Request cancellation of a running job by interrupting its fiber. Returns `true` if a live job fiber was found in
    * THIS process and the interrupt was dispatched, `false` if no such running fiber exists here (unknown id, an
    * already-terminal job, or — under the unsupported multi-server model — a job owned by another instance). The
    * interrupt is best-effort and asynchronous: the fiber's own finalizer records the `Cancelled` terminal state (a
    * blocking JDBC statement in flight runs to completion first), so a `true` result means "cancellation requested",
    * not "already stopped".
    */
  def cancel(id: JobRunId): UIO[Boolean]

  /** Return the most recent jobs ordered by start time descending, up to `limit`. */
  def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]]

  /** Stream the log lines for a job, or `None` if no such job exists (drives the route's 404). The stream tails the
    * per-job log file live and closes once the job is terminal and the tail has reached EOF.
    */
  def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]]

  /** Stream a job's live progress as latest-wins [[ccas.utils.ProgressSnapshot]] JSON frames (one per line), or `None`
    * if no such job exists. Each frame merges the job's own app bars with the shared client's global API gauge. The
    * stream closes when the job is terminal — bars are ephemeral, so nothing is persisted or replayed.
    */
  def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]]
}

object JobRunner {

  // How often the file-tail transport re-polls a job's log file for newly-appended lines while the job is running.
  private val LogTailPollInterval: Duration = 250.millis

  // Floor for the `/progress` sample interval (see `AppSetting.ProgressRefreshIntervalMillis`) — guards a mis-set
  // 0/negative value from busy-looping the sampler. ~60 fps, far tighter than any sensible configured cap.
  private val MinRefreshInterval: Duration = 16.millis

  val live: RLayer[ProgressDisplay & ChessComClient & PostgresClient, JobRunner] =
    ZLayer.scoped {
      for {
        display     <- ZIO.service[ProgressDisplay]
        client      <- ZIO.service[ChessComClient]
        pgClient    <- ZIO.service[PostgresClient]
        completions <- Ref.make(Map.empty[JobRunId, Promise[Nothing, Unit]])
        // Per-job progress channels, registered on submit and dropped on completion — the in-memory analog of
        // `completions`, read by `progressStream` to serve `GET /api/jobs/{id}/progress`.
        jobChannels <- Ref.make(Map.empty[JobRunId, ProgressDisplay.BarChannel])
        // Per-job forked-fiber handles, read by `cancel` to interrupt a running job. Held via a Promise (completed with
        // the fiber the instant `forkIn` returns it) rather than the fiber directly: like `completions`/`jobChannels`
        // the map entry is registered BEFORE the fork, so `release`'s de-register can't race ahead of the register and
        // strand a handle. A `cancel` that arrives in the sub-millisecond gap before the fiber exists simply awaits the
        // Promise. The map is per-process, so `cancel` only reaches fibers in THIS server (see the trait doc / #110).
        runningFibers <- Ref.make(Map.empty[JobRunId, Promise[Nothing, Fiber.Runtime[Throwable, Unit]]])
        // Ids for which an operator `cancel` has been requested. `cancel` adds an id before interrupting its fiber; the
        // job's `onInterrupt` writes `Cancelled` ONLY if its id is here. This distinguishes an operator cancel from the
        // shutdown interrupt `layerScope` fires at every in-flight job on server stop — the latter must leave the row
        // `Running` so the next boot's `markOrphansAsFailed` records it as `Failed`/"Service restarted", not a spurious
        // "Cancelled by operator". `release` clears the id, so the set only ever holds live-and-cancelling jobs.
        cancelRequested <- Ref.make(Set.empty[JobRunId])
        // The layer's own scope owns the job fibers (see `submit`): forking into it detaches each job from the
        // short-lived request fiber that submitted it, and interrupts any still in flight when the server shuts down.
        // `forkIn` self-cleans — a job's child scope closes when its fiber exits — so completed jobs don't accumulate.
        layerScope <- ZIO.scope
        logDir <- ZIO.attempt {
          val dir = Paths.get(ConfigFactory.load().getString("job-logs.directory"))
          Files.createDirectories(dir)
          dir
        }.orDie
        now <- Clock.instant
        _   <- JobRun.markOrphansAsFailed(now).provideEnvironment(zio.ZEnvironment(pgClient))
        days   <- AppSetting.get(AppSetting.JobLogRetentionDays).provideEnvironment(zio.ZEnvironment(pgClient))
        cutoff <- Clock.instant.map(_.minus(days.toLong, ChronoUnit.DAYS))
        swept <- FileSink
          .sweepBefore(logDir, cutoff)
          .tapError(t => ZIO.logWarning(s"Job-log retention sweep failed: ${t.safeMessage}"))
          .orElseSucceed(0)
        _ <- ZIO.logInfo(s"Swept $swept job log(s) older than $days day(s) from $logDir").when(swept > 0)
      } yield new JobRunnerLive(
        display,
        client,
        pgClient,
        completions,
        jobChannels,
        runningFibers,
        cancelRequested,
        layerScope,
        logDir
      )
    }

  private class JobRunnerLive(
    display: ProgressDisplay,
    client: ChessComClient,
    pgClient: PostgresClient,
    completions: Ref[Map[JobRunId, Promise[Nothing, Unit]]],
    jobChannels: Ref[Map[JobRunId, ProgressDisplay.BarChannel]],
    runningFibers: Ref[Map[JobRunId, Promise[Nothing, Fiber.Runtime[Throwable, Unit]]]],
    cancelRequested: Ref[Set[JobRunId]],
    layerScope: Scope,
    logDir: Path
  ) extends JobRunner {

    private val env       = zio.ZEnvironment(display, client, pgClient)
    private val transport = new FileTail(logDir, completions, LogTailPollInterval)

    override def submit(
      kind: JobKind,
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] = {
      // `cancelling` distinguishes a blocker that an operator has already asked to cancel (its id is in `cancelRequested`)
      // but whose interrupt hasn't landed yet — an in-flight blocking statement runs to completion before the fiber
      // flips to `Cancelled` and `release` clears it from the set. Without this, a `ccas ... ` immediately after a
      // Ctrl-C cancel of the same (kind, club) reads the baffling "already running" for a job the operator just killed;
      // the "finishing cancellation — retry in a moment" wording tells them it is self-resolving. (#170)
      def conflictError(cancelling: Boolean): ConflictException = {
        val forClub = clubId.fold("")(c => s" for club $c")
        if (cancelling) { ConflictException(s"A $kind job$forClub is finishing cancellation — retry in a moment") }
        else { ConflictException(s"A $kind job is already running$forClub") }
      }
      (for {
        id <- withTransaction {
          for {
            existing <- JobRun.selectRunningForUpdate(kind, clubId)
            _ <- ZIO.foreachDiscard(existing)(job =>
              cancelRequested.get.flatMap(pending => ZIO.fail(conflictError(pending.contains(job.id))))
            )
            id   = JobRunId.generate()
            now <- Clock.instant
            jobRun = JobRun(id, kind, clubId, trigger, JobRunStatus.Running, params, now, None, None)
            _   <- JobRun.insert(jobRun)
          } yield id
        }
        // Source tag for this job's log lines (`[Kind/slug]`), resolved best-effort — a missing club or read failure
        // degrades to the numeric id, never fails the submit. Resolved before the sink is opened so this interruptible
        // DB read (can block seconds on a Neon cold start) holds no fd: an interrupt here leaks nothing.
        slugOpt <- ZIO.foreach(clubId)(Club.selectId).map(_.flatten.map(_.slug)).catchAll(_ => ZIO.none)
        label = clubId.fold(kind.toString)(cid =>
          s"$kind/${slugOpt.fold(s"#${ClubId.unwrap(cid)}")(ClubSlug.unwrap)}"
        )
        // `FileSink.make` acquires a held-open fd whose only close path is `release` (the child's `.ensuring`); an
        // interrupt between the open and the `forkIn` would leak it, so open + register + fork sit inside one
        // `uninterruptibleMask` (`restore` keeps only the forked job interruptible). `forkIn(layerScope)` — not
        // `.fork` — so the job outlives the submitting request and is interrupted on server shutdown. The completion
        // promise is registered before forking so a log-tail subscribe sees it the instant submit returns; `release`
        // completes + de-registers it in the CHILD fiber, so a single `acquireRelease` bracket doesn't fit.
        // `currentSink.locally` wraps the whole `runJob` so the success/failure handlers' own logs also land in the
        // per-job file; forked children inherit the FiberRef, so parallel work shares the sink.
        _ <- ZIO.uninterruptibleMask { restore =>
          for {
            sink    <- FileSink.make(logDir, JobRunId.unwrap(id))
            promise <- Promise.make[Nothing, Unit]
            // The per-job bar channel: app bars created inside this job (via `currentChannel.locally` below) publish
            // here; `progressStream` merges it with the global API gauge to serve this job's `/progress`.
            channel <- SubscriptionRef.make(Map.empty[Int, BarSnapshot])
            // Runs in the CHILD fiber when the job ends (success, failure, or interruption): close the sink (flush +
            // close the held-open writer) FIRST, then fire the completion promise, then de-register — so a tailer
            // observes a fully written, closed file before it sees the job done, and always gets EOF, incl. on shutdown.
            // The channel de-registers too: `/progress`'s `interruptWhen(promise)` already ends live followers, and a
            // later subscribe finds no channel (job terminal) and emits a single settling frame.
            // Registered before the fork (see `runningFibers`) so the fiber handle is addressable the instant the job is
            // live; `fiberSlot` is completed with the fiber below.
            fiberSlot <- Promise.make[Nothing, Fiber.Runtime[Throwable, Unit]]
            release = for {
              _ <- sink.close()
              _ <- promise.succeed(())
              _ <- completions.update(_ - id)
              _ <- jobChannels.update(_ - id)
              _ <- runningFibers.update(_ - id)
              _ <- cancelRequested.update(_ - id)
            } yield ()
            _ <- completions.update(_ + (id -> promise))
            _ <- jobChannels.update(_ + (id -> channel))
            _ <- runningFibers.update(_ + (id -> fiberSlot))
            fiber <- restore(
              // `installLogger` forces ProgressDisplay's ZLogger onto the job fiber itself, so the per-job FileSink is
              // written regardless of which fiber submitted the job. `forkIn` inherits the submitter's loggers, and an
              // HTTP request handler carries only the default logger (not the app-scoped ProgressDisplay one) — so
              // without this, HTTP-submitted jobs logged to the default logger and their per-job file stayed empty (#132).
              // `currentChannel.locally` scopes bar publishing to this job's channel for the whole run (forked children
              // inherit the FiberRef), mirroring `currentSink` for log lines.
              display.installLogger(
                ProgressDisplay.sourced(label)(
                  ProgressDisplay.currentChannel.locally(Some(channel))(
                    JobLogSink.currentSink
                      .locally(sink)(
                        runJob(id, effect(Some(id))).ensuring(release)
                      )
                  )
                )
              )
            ).forkIn(layerScope)
            _ <- fiberSlot.succeed(fiber)
          } yield ()
        }
      } yield id).catchSome {
        // Phantom-row race: two transactions both saw no running job, the unique partial index
        // on (kind, COALESCE(club_id, -1)) WHERE status = 'Running' caught the second insert. A genuine concurrent
        // submit (not a cancel-in-flight), so the plain "already running" wording is right.
        case e: SQLException if e.getSQLState == "23505" => ZIO.fail(conflictError(cancelling = false))
      }
    }

    private def runJob(
      id: JobRunId,
      effect: RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): UIO[Unit] =
      def onFailure(error: Throwable): UIO[Unit] = {
        val msg = error.safeMessage
        Clock.instant.flatMap(now =>
          JobRun.updateStatus(id, JobRunStatus.Failed, Some(now), Some(msg))
            .provideEnvironment(env)
            .unit.catchAll(e => ZIO.logError(s"Failed to record job failure: ${e.safeMessage}"))
        )
      }

      def onSuccess: UIO[Unit] =
        Clock.instant.flatMap(now =>
          JobRun.updateStatus(id, JobRunStatus.Completed, Some(now), None)
            .provideEnvironment(env)
            .unit.catchAll(e => ZIO.logError(s"Failed to record job completion: ${e.safeMessage}"))
        )

      // Interruption is a `Cause`, not a typed error, so it bypasses `foldZIO`'s branches entirely. This hook fires on
      // ANY interruption — but only an operator `cancel` (which registers the id in `cancelRequested` before
      // interrupting) should record `Cancelled`. The other interrupt source is `layerScope` tearing down every in-flight
      // job on server shutdown; those must be left `Running` so the next boot's `markOrphansAsFailed` records them as
      // `Failed`/"Service restarted", not "Cancelled by operator". The finalizer runs uninterruptibly so the write lands;
      // `markCancelled`'s `WHERE status = Running` guard makes it a no-op if the job reached a terminal state first.
      def onCancelled: UIO[Unit] =
        ZIO.whenZIODiscard(cancelRequested.get.map(_.contains(id)))(
          Clock.instant.flatMap(now =>
            JobRun.markCancelled(id, now)
              .provideEnvironment(env)
              .unit.catchAll(e => ZIO.logError(s"Failed to record job cancellation: ${e.safeMessage}"))
          )
        )

      effect.provideEnvironment(env).foldZIO(onFailure, _ => onSuccess).onInterrupt(onCancelled)

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] =
      JobRun.selectId(id)

    // Interrupt the job's fiber if it is running in this process. Registers the id in `cancelRequested` first so the
    // fiber's `onInterrupt` knows this is an operator cancel (not a shutdown) and records `Cancelled`; then
    // `interruptFork` dispatches the interrupt in the background so the caller (the HTTP handler) returns promptly
    // rather than blocking until the fiber unwinds. `slot.await` resolves the fiber the instant the fork registered it.
    // `fiber.poll` guards the narrow window where the job already finished but `release` hasn't de-registered the slot
    // yet: a completed fiber returns `false` (nothing to cancel) rather than a misleading `true`.
    override def cancel(id: JobRunId): UIO[Boolean] =
      runningFibers.get.map(_.get(id)).flatMap {
        case None => ZIO.succeed(false)
        case Some(slot) =>
          slot.await.flatMap(fiber =>
            fiber.poll.flatMap {
              case Some(_) => ZIO.succeed(false) // already terminal — release just hasn't cleared the slot yet
              case None    => (cancelRequested.update(_ + id) *> fiber.interruptFork).as(true)
            }
          )
      }

    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] =
      JobRun.selectRecent(limit)

    override def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
      status(id).map(_.map(_ => transport.subscribe(id)))

    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
      status(id).flatMap {
        case None => ZIO.none
        case Some(_) =>
          for {
            chans <- jobChannels.get
            comps <- completions.get
            // The refresh cap is a non-essential tuning knob for best-effort bars — a failed settings read (transient DB
            // error) falls back to the compiled-in default rather than failing the `/progress` subscribe.
            ms <- AppSetting
              .get(AppSetting.ProgressRefreshIntervalMillis)
              .orElseSucceed(AppSetting.ProgressRefreshIntervalMillis.default)
          } yield Some(progressFrames(chans.get(id), comps.get(id), ms.millis))
      }

    // Build the `/progress` frame stream by SAMPLING the merged bar state (the shared global gauge channel plus this
    // job's channel — later keys win; a job never shares a bar id with the gauge, so the union is disjoint) at
    // `refreshInterval` and de-duplicating consecutive-identical samples. Sampling + `changes` gives conflation: a busy
    // job's rapid updates collapse to at most one frame per interval (bounding the encode + send the operator asked to
    // cap), an idle job emits nothing (consecutive samples are equal), and — unlike dropping excess — the LATEST state
    // is always delivered within one interval, so a bar that ticks once then goes quiet still reaches the follower.
    // Frames render bar-id-ordered (creation order: gauge first, then app bars). The stream ends when the job completes;
    // a terminal job (no live channel / promise) emits a single settling frame and closes.
    private def progressFrames(
      jobChannel: Option[ProgressDisplay.BarChannel],
      completion: Option[Promise[Nothing, Unit]],
      refreshInterval: Duration
    ): ZStream[Any, Throwable, String] = {
      val readState: UIO[Map[Int, BarSnapshot]] =
        for {
          global <- display.globalBarSnapshot
          job    <- jobChannel.fold(ZIO.succeed(Map.empty[Int, BarSnapshot]))(_.get)
        } yield global ++ job
      // Floor the interval so a mis-set 0/negative can't busy-loop the sampler.
      val interval = if (refreshInterval.toNanos > MinRefreshInterval.toNanos) { refreshInterval } else { MinRefreshInterval }
      val frames =
        ZStream
          .fromZIO(readState)
          .repeat(Schedule.spaced(interval))
          .changes
          .map(m => ProgressSnapshot(m.values.toList.sortBy(_.id)).toJson)
      completion match {
        case Some(p) => frames.interruptWhen(p.await)
        case None    => frames.take(1)
      }
    }
  }
}
