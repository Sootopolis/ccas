package ccas.server.jobs

import java.nio.file.{Files, Path, Paths}
import java.sql.SQLException

import com.typesafe.config.ConfigFactory

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.stream.ZStream
import zio.{Clock, Duration, Promise, RIO, RLayer, Ref, Scope, UIO, ZIO, ZLayer, durationInt}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.{ClubId, JobRunId}
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
        // Fork the job into the layer scope (not the submitting fiber) so it outlives the request that submitted it;
        // plain `.fork` would make it a child of the request handler fiber and structured concurrency would interrupt
        // it the moment the HTTP response is sent. `forkIn` also interrupts any still-running job when the layer scope
        // closes on server shutdown, and self-cleans on normal completion, so it's the sole job-lifecycle mechanism.
        //
        // Register the completion promise in the PARENT, before forking, so a log-tail subscribe issued the instant
        // submit returns always sees the promise. The release (complete promise + de-register) runs in the CHILD fiber
        // via `ensuring` — acquire and release live in different fibers, so a single `acquireRelease` bracket doesn't
        // fit. `uninterruptibleMask` makes the register→fork pair atomic (an interrupt in between would leave a promise
        // registered but never completed, hanging a later subscribe); `restore` keeps the forked job interruptible.
        //
        // `currentSink.locally` wraps the entire `runJob` (not just the user effect) so the success/failure handlers'
        // own `ZIO.logError` calls also land in the per-job file. FiberRef values are inherited by forked children, so
        // any parallel work the job spawns sees the same sink. The `ensuring` closes the sink (flushing + closing the
        // held-open writer) and only then fires the completion promise, so the tailer observes a fully written, closed
        // file before it sees the job as done; it runs on success, failure, and interruption, so a tailing client
        // always gets EOF — including on shutdown.
        _ <- ZIO.uninterruptibleMask { restore =>
          completions.update(_ + (id -> promise)) *>
            restore(
              JobLogSink.currentSink
                .locally(sink)(
                  runJob(id, effect(Some(id)))
                    .ensuring(sink.close() *> promise.succeed(()) *> completions.update(_ - id))
                )
            ).forkIn(layerScope)
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
