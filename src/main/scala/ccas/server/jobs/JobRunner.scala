package ccas.server.jobs

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.{Fiber, RIO, RLayer, Ref, UIO, ZIO, ZLayer}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.safeMessage
import ccas.utils.CcasLogger

/** Asynchronous job executor that runs analysis tasks as forked fibers.
  *
  * Each submitted job is tracked in the `job_run` database table with a ULID identifier. Only one job of a given kind
  * (optionally scoped to a club) may run at a time; duplicate submissions are rejected with a [[JobConflictException]].
  */
trait JobRunner {

  /** Fork a new job and return its ID. Fails with [[JobConflictException]] if a matching job is already running.
    *
    * The effect receives the job run ID (as a string) so that analysis apps can link their own run records back to the
    * server-level job.
    */
  def submit(
    kind: JobKind,
    clubId: Option[ClubId],
    params: Option[String],
    trigger: RunTrigger,
    effect: Option[JobRunId] => RIO[CcasLogger & ChessComClient & Transactor, Any]
  ): RIO[Transactor, JobRunId]

  /** Look up a job by ID, returning `None` if no such job exists. */
  def status(id: JobRunId): RIO[Transactor, Option[JobRun]]

  /** Return the most recent jobs ordered by start time descending, up to `limit`. */
  def recentJobs(limit: Int): RIO[Transactor, List[JobRun]]
}

object JobRunner {

  val live: RLayer[CcasLogger & ChessComClient & Transactor, JobRunner] =
    ZLayer.scoped {
      for {
        logger <- ZIO.service[CcasLogger]
        client <- ZIO.service[ChessComClient]
        xa     <- ZIO.service[Transactor]
        fibers <- Ref.make(Set.empty[Fiber.Runtime[Nothing, Unit]])
        _      <- JobRun.markOrphansAsFailed.provideEnvironment(zio.ZEnvironment(xa))
        runner = new JobRunnerLive(logger, client, xa, fibers)
        _ <- ZIO.addFinalizer(runner.awaitAll)
      } yield runner
    }

  private class JobRunnerLive(
    logger: CcasLogger,
    client: ChessComClient,
    xa: Transactor,
    fibers: Ref[Set[Fiber.Runtime[Nothing, Unit]]]
  ) extends JobRunner {

    private val env = zio.ZEnvironment(logger, client, xa)

    override def submit(
      kind: JobKind,
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[CcasLogger & ChessComClient & Transactor, Any]
    ): RIO[Transactor, JobRunId] =
      for {
        existing <- JobRun.selectRunning(kind, clubId)
        _ <- ZIO.whenDiscard(existing.isDefined)(
          ZIO.fail(
            new JobConflictException(
              s"A ${kind} job is already running" +
                clubId.fold("")(c => s" for club $c")
            )
          )
        )
        id     = JobRunId.generate()
        now    = Instant.now()
        jobRun = JobRun(id, kind, clubId, trigger, JobRunStatus.Running, params, now, None, None)
        _     <- JobRun.insert(jobRun)
        fiber <- runJob(id, effect(Some(id))).fork
        _     <- fibers.update(_ + fiber)
      } yield id

    private def runJob(
      id: JobRunId,
      effect: RIO[CcasLogger & ChessComClient & Transactor, Any]
    ): UIO[Unit] =
      def onFailure(error: Throwable): UIO[Unit] = {
        val msg = error.safeMessage
        JobRun.updateStatus(id, JobRunStatus.Failed, Some(Instant.now()), Some(msg))
          .provideEnvironment(env)
          .unit.orDie
      }

      def onSuccess: UIO[Unit] =
        JobRun.updateStatus(id, JobRunStatus.Completed, Some(Instant.now()), None)
          .provideEnvironment(env)
          .unit.orDie

      effect.provideEnvironment(env).foldZIO(onFailure, _ => onSuccess)

    def awaitAll: UIO[Unit] =
      fibers.get.flatMap(fs => ZIO.foreachDiscard(fs)(_.await))

    override def status(id: JobRunId): RIO[Transactor, Option[JobRun]] =
      JobRun.selectId(id)

    override def recentJobs(limit: Int): RIO[Transactor, List[JobRun]] =
      JobRun.selectRecent(limit)
  }
}
