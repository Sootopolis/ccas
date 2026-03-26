package ccas.server.jobs

import java.time.Instant
import com.augustnagro.magnum.Transactor
import zio.{Fiber, RIO, RLayer, Ref, UIO, ZIO, ZLayer}
import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.CcasLogger
import ccas.utils.client.ChessComClient
import ccas.utils.errors.safeMessage

trait JobRunner {
  def submit(
    kind: JobKind,
    clubSlug: Option[ClubSlug],
    params: Option[String],
    trigger: RunTrigger,
    effect: RIO[CcasLogger & ChessComClient & Transactor, Any]
  ): RIO[Transactor, JobRunId]

  def status(id: JobRunId): RIO[Transactor, Option[JobRun]]

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

  private class JobRunnerLive(logger: CcasLogger, client: ChessComClient, xa: Transactor, fibers: Ref[Set[Fiber.Runtime[Nothing, Unit]]])
      extends JobRunner {

    private val env = zio.ZEnvironment(logger, client, xa)

    override def submit(
      kind: JobKind,
      clubSlug: Option[ClubSlug],
      params: Option[String],
      trigger: RunTrigger,
      effect: RIO[CcasLogger & ChessComClient & Transactor, Any]
    ): RIO[Transactor, JobRunId] =
      for {
        existing <- JobRun.selectRunning(kind, clubSlug)
        _ <- ZIO.whenDiscard(existing.isDefined)(
          ZIO.fail(
            new JobConflictException(
              s"A ${kind} job is already running" +
                clubSlug.fold("")(c => s" for club $c")
            )
          )
        )
        id     = JobRunId.generate()
        now    = Instant.now()
        jobRun = JobRun(id, kind, trigger, JobRunStatus.Running, clubSlug, params, now, None, None)
        _     <- JobRun.insert(jobRun)
        fiber <- runJob(id, kind, effect).fork
        _     <- fibers.update(_ + fiber)
      } yield id

    private def runJob(
      id: JobRunId,
      kind: JobKind,
      effect: RIO[CcasLogger & ChessComClient & Transactor, Any]
    ): UIO[Unit] =
      def onFailure(error: Throwable): UIO[Unit] = {
        val msg = error.safeMessage
        JobRun.updateStatus(id, JobRunStatus.Failed, Some(Instant.now()), Some(msg))
          .provideEnvironment(env)
          .unit.orDie
      }

      def onSuccess: UIO[Unit] = {
        val complete =
          JobRun.updateStatus(id, JobRunStatus.Completed, Some(Instant.now()), None)
            .provideEnvironment(env)
            .unit.orDie
        val followUp =
          ZIO.whenDiscard(kind == JobKind.Recruitment || kind == JobKind.Membership || kind == JobKind.History)(
            submitRef.provideEnvironment(env).ignore
          )
        complete *> followUp
      }

      effect.provideEnvironment(env).foldZIO(onFailure, _ => onSuccess)

    private def submitRef: RIO[Transactor, Unit] = {
      import ccas.analysis.apps.ref.RefApp
      submit(JobKind.MatchRef, None, None, RunTrigger.FollowUp, RefApp.populate(RunTrigger.FollowUp)).ignore
    }

    def awaitAll: UIO[Unit] =
      fibers.get.flatMap(fs => ZIO.foreachDiscard(fs)(_.await))

    override def status(id: JobRunId): RIO[Transactor, Option[JobRun]] =
      JobRun.selectId(id)

    override def recentJobs(limit: Int): RIO[Transactor, List[JobRun]] =
      JobRun.selectRecent(limit)
  }
}
