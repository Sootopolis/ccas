package ccas.server.scheduler

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.{durationInt, Ref, RIO, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.ServerTables
import ccas.server.jobs.{JobKind, JobRun, JobRunner}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient
import ccas.utils.{CcasLogger, TestCcasLogger}

object TestJobScheduler extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobScheduler")(
    testPollFiberStopsOnScopeClose,
    testDueScheduleSubmitted,
    testNotYetDueScheduleSkipped,
    testDisabledScheduleSkipped,
    testErrorDoesNotCrashScheduler
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

  /** JobRunner stub that tracks which clubIds are submitted. */
  private def trackingRunner(submitted: Ref[List[Option[ClubId]]]): JobRunner = new JobRunner {
    override def submit(
      kind: JobKind,
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[CcasLogger & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      submitted.update(clubId :: _).as(JobRunId.generate())

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
      countAfter - countAtClose <= 1 // at most one in-flight poll may complete after scope closed
    )
  }

  private def testDueScheduleSubmitted = test("submits job when schedule is due") {
    val dueClubId = ClubId(301)
    for {
      pgClient  <- ZIO.service[PostgresClient]
      submitted <- Ref.make(List.empty[Option[ClubId]])
      runner = trackingRunner(submitted)
      scheduler = new JobScheduler.JobSchedulerLive(TestCcasLogger.noop, runner, pgClient, 50.millis)

      twoHoursAgo = Instant.now().minus(2, ChronoUnit.HOURS)
      _ <- Club.upsert(Club(dueClubId, twoHoursAgo, ClubSlug("due-test"), "Due Test"))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(dueClubId), None, intervalHours = 1, enabled = true, lastRunAt = Some(twoHoursAgo))
      )
      _ <- ZIO.scoped {
        scheduler.start *> ZIO.sleep(200.millis)
      }
      clubs <- submitted.get
      dueCount = clubs.count(_.contains(dueClubId))
    } yield assertTrue(dueCount >= 1)
  }

  private def testNotYetDueScheduleSkipped = test("skips schedule that is not yet due") {
    val notDueClubId = ClubId(302)
    for {
      pgClient  <- ZIO.service[PostgresClient]
      submitted <- Ref.make(List.empty[Option[ClubId]])
      runner = trackingRunner(submitted)
      scheduler = new JobScheduler.JobSchedulerLive(TestCcasLogger.noop, runner, pgClient, 50.millis)

      justNow = Instant.now()
      _ <- Club.upsert(Club(notDueClubId, justNow, ClubSlug("notdue-test"), "Not Due Test"))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(notDueClubId), None, intervalHours = 24, enabled = true, lastRunAt = Some(justNow))
      )
      _ <- ZIO.scoped {
        scheduler.start *> ZIO.sleep(200.millis)
      }
      clubs <- submitted.get
      notDueCount = clubs.count(_.contains(notDueClubId))
    } yield assertTrue(notDueCount == 0)
  }

  private def testDisabledScheduleSkipped = test("skips disabled schedule") {
    val disabledClubId = ClubId(303)
    for {
      pgClient  <- ZIO.service[PostgresClient]
      submitted <- Ref.make(List.empty[Option[ClubId]])
      runner = trackingRunner(submitted)
      scheduler = new JobScheduler.JobSchedulerLive(TestCcasLogger.noop, runner, pgClient, 50.millis)

      _ <- Club.upsert(Club(disabledClubId, Instant.now(), ClubSlug("disabled-test"), "Disabled Test"))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(disabledClubId), None, intervalHours = 0, enabled = false, lastRunAt = None)
      )
      _ <- ZIO.scoped {
        scheduler.start *> ZIO.sleep(200.millis)
      }
      clubs <- submitted.get
      disabledCount = clubs.count(_.contains(disabledClubId))
    } yield assertTrue(disabledCount == 0)
  }

  private def testErrorDoesNotCrashScheduler = test("error in submission does not crash scheduler") {
    val errorClubId = ClubId(304)
    for {
      pgClient <- ZIO.service[PostgresClient]
      callCount <- Ref.make(0)
      failingRunner = new JobRunner {
        override def submit(
          kind: JobKind,
          clubId: Option[ClubId],
          params: Option[String],
          trigger: RunTrigger,
          effect: Option[JobRunId] => RIO[CcasLogger & ChessComClient & PostgresClient, Any]
        ): RIO[PostgresClient, JobRunId] =
          callCount.update(_ + 1) *> ZIO.fail(new RuntimeException("boom"))

        override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
        override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
      }
      scheduler = new JobScheduler.JobSchedulerLive(TestCcasLogger.noop, failingRunner, pgClient, 50.millis)

      _ <- Club.upsert(Club(errorClubId, Instant.now(), ClubSlug("error-test"), "Error Test"))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(errorClubId), None, intervalHours = 0, enabled = true, lastRunAt = None)
      )
      _ <- ZIO.scoped {
        scheduler.start *> ZIO.sleep(400.millis)
      }
      count <- callCount.get
    } yield assertTrue(count >= 2) // scheduler survived the error and polled again
  }
}
