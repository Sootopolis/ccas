package ccas.server.jobs

import java.sql.SQLException
import java.time.Instant

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.{Fiber, RIO, RLayer, Ref, UIO, ZIO, ZLayer}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{ConflictException, safeMessage}
import ccas.utils.CcasLogger

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
    effect: Option[JobRunId] => RIO[CcasLogger & ChessComClient & PostgresClient, Any]
  ): RIO[PostgresClient, JobRunId]

  /** Look up a job by ID, returning `None` if no such job exists. */
  def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]]

  /** Return the most recent jobs ordered by start time descending, up to `limit`. */
  def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]]
}

object JobRunner {

  val live: RLayer[CcasLogger & ChessComClient & PostgresClient, JobRunner] =
    ZLayer.scoped {
      for {
        logger <- ZIO.service[CcasLogger]
        client <- ZIO.service[ChessComClient]
        pgClient <- ZIO.service[PostgresClient]
        fibers   <- Ref.make(Set.empty[Fiber.Runtime[Nothing, Unit]])
        _        <- JobRun.markOrphansAsFailed.provideEnvironment(zio.ZEnvironment(pgClient))
        runner = new JobRunnerLive(logger, client, pgClient, fibers)
        _ <- ZIO.addFinalizer(runner.awaitAll)
      } yield runner
    }

  private class JobRunnerLive(
    logger: CcasLogger,
    client: ChessComClient,
    pgClient: PostgresClient,
    fibers: Ref[Set[Fiber.Runtime[Nothing, Unit]]]
  ) extends JobRunner {

    private val env = zio.ZEnvironment(logger, client, pgClient)

    override def submit(
      kind: JobKind,
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[CcasLogger & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] = {
      def conflictError = ConflictException(
        s"A ${kind} job is already running" + clubId.fold("")(c => s" for club $c")
      )
      (for {
        id <- withTransaction {
          for {
            existing <- JobRun.selectRunningForUpdate(kind, clubId)
            _ <- ZIO.whenDiscard(existing.isDefined)(ZIO.fail(conflictError))
            id     = JobRunId.generate()
            now    = Instant.now()
            jobRun = JobRun(id, kind, clubId, trigger, JobRunStatus.Running, params, now, None, None)
            _     <- JobRun.insert(jobRun)
          } yield id
        }
        fiber <- runJob(id, effect(Some(id))).fork
        _     <- fibers.update(_ + fiber)
      } yield id).catchSome {
        // Phantom-row race: two transactions both saw no running job, the unique partial index
        // on (kind, COALESCE(club_id, -1)) WHERE status = 'Running' caught the second insert.
        case e: SQLException if e.getSQLState == "23505" => ZIO.fail(conflictError)
      }
    }

    private def runJob(
      id: JobRunId,
      effect: RIO[CcasLogger & ChessComClient & PostgresClient, Any]
    ): UIO[Unit] =
      def onFailure(error: Throwable): UIO[Unit] = {
        val msg = error.safeMessage
        JobRun.updateStatus(id, JobRunStatus.Failed, Some(Instant.now()), Some(msg))
          .provideEnvironment(env)
          .unit.catchAll(e => ZIO.logError(s"Failed to record job failure: ${e.safeMessage}"))
      }

      def onSuccess: UIO[Unit] =
        JobRun.updateStatus(id, JobRunStatus.Completed, Some(Instant.now()), None)
          .provideEnvironment(env)
          .unit.catchAll(e => ZIO.logError(s"Failed to record job completion: ${e.safeMessage}"))

      effect.provideEnvironment(env).foldZIO(onFailure, _ => onSuccess)

    def awaitAll: UIO[Unit] =
      fibers.get.flatMap(fs => ZIO.foreachDiscard(fs)(_.await))

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] =
      JobRun.selectId(id)

    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] =
      JobRun.selectRecent(limit)
  }
}
