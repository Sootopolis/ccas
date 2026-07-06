package ccas.server.jobs

import java.nio.file.{Files, Path, Paths}
import java.sql.SQLException
import java.time.temporal.ChronoUnit

import com.typesafe.config.ConfigFactory

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.stream.ZStream
import zio.{Clock, Duration, Promise, RIO, RLayer, Ref, Scope, UIO, ZIO, ZLayer, durationInt}

import ccas.analysis.tables.{AppSetting, Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{ConflictException, safeMessage}
import ccas.utils.{JobLogSink, ProgressDisplay}

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

  /** Return the most recent jobs ordered by start time descending, up to `limit`. */
  def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]]

  /** Stream the log lines for a job, or `None` if no such job exists (drives the route's 404). The stream tails the
    * per-job log file live and closes once the job is terminal and the tail has reached EOF.
    */
  def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]]
}

object JobRunner {

  // How often the file-tail transport re-polls a job's log file for newly-appended lines while the job is running.
  private val LogTailPollInterval: Duration = 250.millis

  val live: RLayer[ProgressDisplay & ChessComClient & PostgresClient, JobRunner] =
    ZLayer.scoped {
      for {
        display     <- ZIO.service[ProgressDisplay]
        client      <- ZIO.service[ChessComClient]
        pgClient    <- ZIO.service[PostgresClient]
        completions <- Ref.make(Map.empty[JobRunId, Promise[Nothing, Unit]])
        // The layer's own scope owns the job fibers (see `submit`): forking into it detaches each job from the
        // short-lived request fiber that submitted it, and interrupts any still in flight when the server shuts down.
        // `forkIn` self-cleans — a job's child scope closes when its fiber exits — so completed jobs don't accumulate.
        layerScope <- ZIO.scope
        logDir <- ZIO.attempt {
          val dir = Paths.get(ConfigFactory.load().getString("job-logs.directory"))
          Files.createDirectories(dir)
          dir
        }.orDie
        _ <- JobRun.markOrphansAsFailed.provideEnvironment(zio.ZEnvironment(pgClient))
        days   <- AppSetting.get(AppSetting.JobLogRetentionDays).provideEnvironment(zio.ZEnvironment(pgClient))
        cutoff <- Clock.instant.map(_.minus(days.toLong, ChronoUnit.DAYS))
        swept <- FileSink
          .sweepBefore(logDir, cutoff)
          .tapError(t => ZIO.logWarning(s"Job-log retention sweep failed: ${t.safeMessage}"))
          .orElseSucceed(0)
        _ <- ZIO.logInfo(s"Swept $swept job log(s) older than $days day(s) from $logDir").when(swept > 0)
      } yield new JobRunnerLive(display, client, pgClient, completions, layerScope, logDir)
    }

  private class JobRunnerLive(
    display: ProgressDisplay,
    client: ChessComClient,
    pgClient: PostgresClient,
    completions: Ref[Map[JobRunId, Promise[Nothing, Unit]]],
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
      def conflictError = ConflictException(
        s"A $kind job is already running" + clubId.fold("")(c => s" for club $c")
      )
      (for {
        id <- withTransaction {
          for {
            existing <- JobRun.selectRunningForUpdate(kind, clubId)
            _ <- ZIO.whenDiscard(existing.isDefined)(ZIO.fail(conflictError))
            id   = JobRunId.generate()
            now <- Clock.instant
            jobRun = JobRun(id, kind, clubId, trigger, JobRunStatus.Running, params, now, None, None)
            _   <- JobRun.insert(jobRun)
          } yield id
        }
        sink    <- FileSink.make(logDir, JobRunId.unwrap(id))
        promise <- Promise.make[Nothing, Unit]
        // Runs in the CHILD fiber when the job ends (success, failure, or interruption): close the sink (flush + close
        // the held-open writer) FIRST, then fire the completion promise, then de-register — so a tailer observes a
        // fully written, closed file before it sees the job done, and always gets EOF, including on shutdown.
        release = for {
          _ <- sink.close()
          _ <- promise.succeed(())
          _ <- completions.update(_ - id)
        } yield ()
        // Source tag for this job's log lines (`[Kind/slug]`), resolved best-effort — a missing club or a
        // read failure degrades to the numeric id, never fails the submit. Cosmetic, so it stays outside
        // the `withTransaction` block above.
        slugOpt <- ZIO.foreach(clubId)(Club.selectId).map(_.flatten.map(_.slug)).catchAll(_ => ZIO.none)
        label = clubId.fold(kind.toString)(cid =>
          s"$kind/${slugOpt.fold(s"#${ClubId.unwrap(cid)}")(ClubSlug.unwrap)}"
        )
        // Fork the job into the layer scope (not the submitting fiber) so it outlives the request that submitted it;
        // plain `.fork` would make it a child of the request handler fiber and structured concurrency would interrupt
        // it the moment the HTTP response is sent. `forkIn` also interrupts any still-running job when the layer scope
        // closes on server shutdown, and self-cleans on normal completion, so it's the sole job-lifecycle mechanism.
        //
        // Register the completion promise in the PARENT, before forking, so a log-tail subscribe issued the instant
        // submit returns always sees the promise. `release` (run via `ensuring`) completes the promise + de-registers
        // in the CHILD fiber — acquire and release live in different fibers, so a single `acquireRelease` bracket
        // doesn't fit. `uninterruptibleMask` makes the register→fork pair atomic (an interrupt in between would leave a
        // promise registered but never completed, hanging a later subscribe); `restore` keeps the forked job interruptible.
        //
        // `currentSink.locally` wraps the entire `runJob` (not just the user effect) so the success/failure handlers'
        // own `ZIO.logError` calls also land in the per-job file. FiberRef values are inherited by forked children, so
        // any parallel work the job spawns sees the same sink.
        _ <- ZIO.uninterruptibleMask { restore =>
          for {
            _ <- completions.update(_ + (id -> promise))
            _ <- restore(
              // `installLogger` forces ProgressDisplay's ZLogger onto the job fiber itself, so the per-job FileSink is
              // written regardless of which fiber submitted the job. `forkIn` inherits the submitter's loggers, and an
              // HTTP request handler carries only the default logger (not the app-scoped ProgressDisplay one) — so
              // without this, HTTP-submitted jobs logged to the default logger and their per-job file stayed empty (#132).
              display.installLogger(
                ProgressDisplay.sourced(label)(
                  JobLogSink.currentSink
                    .locally(sink)(
                      runJob(id, effect(Some(id))).ensuring(release)
                    )
                )
              )
            ).forkIn(layerScope)
          } yield ()
        }
      } yield id).catchSome {
        // Phantom-row race: two transactions both saw no running job, the unique partial index
        // on (kind, COALESCE(club_id, -1)) WHERE status = 'Running' caught the second insert.
        case e: SQLException if e.getSQLState == "23505" => ZIO.fail(conflictError)
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

      effect.provideEnvironment(env).foldZIO(onFailure, _ => onSuccess)

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] =
      JobRun.selectId(id)

    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] =
      JobRun.selectRecent(limit)

    override def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
      status(id).map(_.map(_ => transport.subscribe(id)))
  }
}
