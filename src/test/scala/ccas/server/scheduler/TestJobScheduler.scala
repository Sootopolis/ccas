package ccas.server.scheduler

import zio.{durationInt, Ref, RIO, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.ServerTables
import ccas.server.jobs.{JobKind, JobRun, JobRunner}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.{CcasLogger, TestCcasLogger}
import ccas.utils.sql.PostgresClient

object TestJobScheduler extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobScheduler")(
    testPollFiberStopsOnScopeClose
  ).provideShared(
    FreshSchemaLayer("test_scheduler", onInit = ServerTables.ensureTables)
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.seconds)

  private val clubId = ClubId(300)

  /** JobRunner stub that counts submissions without running effects. */
  private def stubRunner(submissions: Ref[Int]): JobRunner = new JobRunner {
    override def submit(
      kind: JobKind,
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[CcasLogger & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      submissions.update(_ + 1).as(JobRunId.generate())

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
  }

  private def testPollFiberStopsOnScopeClose = test("poll fiber stops when enclosing scope closes") {
    for {
      pgClient    <- ZIO.service[PostgresClient]
      submissions <- Ref.make(0)
      runner = stubRunner(submissions)
      scheduler = new JobScheduler.JobSchedulerLive(TestCcasLogger.noop, runner, pgClient, 50.millis)

      // Seed: a club and a schedule that is always due (intervalHours = 0, so every poll triggers)
      _ <- Club.upsert(Club(clubId, java.time.Instant.parse("2025-01-01T00:00:00Z"), ClubSlug("sched-test"), "Sched Test"))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(clubId), None, intervalHours = 0, enabled = true, lastRunAt = None)
      )

      // Start the scheduler in a scope, let it poll several times, then close the scope
      _ <- ZIO.scoped {
        scheduler.start *> ZIO.sleep(400.millis)
      }

      // The scope has closed — the poll fiber should have been interrupted
      countAtClose <- submissions.get
      _ <- ZIO.sleep(400.millis)
      countAfter <- submissions.get
    } yield assertTrue(
      countAtClose >= 2,           // scheduler was actively polling while scope was open
      countAfter == countAtClose   // no further polls after scope closed
    )
  }
}
