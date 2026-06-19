package ccas.server.scheduler

import java.time.temporal.ChronoUnit

import zio.{durationInt, Clock, Duration, Ref, RIO, ZIO}
import zio.stream.ZStream
import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, TestClock, ZIOSpecDefault}

import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.ServerTables
import ccas.server.jobs.{JobKind, JobRun, JobRunner}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ConflictException
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient, TestDbCleanup}
import ccas.utils.ProgressDisplay

object TestJobScheduler extends ZIOSpecDefault {

  // Shared schema (`provideShared`) means every test's scheduler instance sees every other test's
  // `job_schedule` rows. `@@ sequential` keeps schedulers from poaching each other's rows mid-poll;
  // `@@ before(clearJobSchedules)` keeps the leftover-rows count from defeating per-test assertions
  // (notably `testErrorDoesNotCrashScheduler`, which only proves "scheduler survived the error" if
  // `callCount` advances across two separate poll iterations — leftover always-due rows would let
  // a single iteration satisfy the assertion). Production behaviour is unaffected; this is purely
  // test fixture isolation. See memory `project_flaky_job_scheduler_test.md`.
  override def spec: Spec[Any, Throwable] = (
    suite("TestJobScheduler")(
      testPollFiberStopsOnScopeClose,
      testDueScheduleSubmitted,
      testNotYetDueScheduleSkipped,
      testDisabledScheduleSkipped,
      testErrorDoesNotCrashScheduler,
      testErrorInOneScheduleDoesNotBlockOthers,
      testConflictKeepsPollingAndDoesNotAdvanceLastRunAt
    )
      @@ TestAspect.before(TestDbCleanup.clearJobSchedules)
      @@ TestAspect.sequential
      @@ TestAspect.timeout(30.seconds)
  ).provideShared(
    FreshSchemaLayer("test_scheduler", onInit = ServerTables.ensureTables)
  )

  // Virtual poll interval driven by TestClock; no wall-clock dependency. Each test advances by
  // `advanceWindow` in one go, relying on `Schedule.fixed`'s catch-up behavior to fire overdue
  // iterations back-to-back. A single `pollInterval` advance would suffice now that tests run
  // sequentially against a clean `job_schedule`; the 5× window stays as defensive headroom (it lets
  // `testErrorDoesNotCrashScheduler` watch `callCount` cross 2 across distinct iterations).
  private val pollInterval: Duration  = 1.minute
  private val advanceWindow: Duration = pollInterval.multipliedBy(5)

  /** JobRunner stub that counts submissions without running effects. */
  private def stubRunner(submissions: Ref[Int]): JobRunner = new JobRunner {
    override def submit(
      kind: JobKind,
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      submissions.update(_ + 1).as(JobRunId.generate())

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
    override def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = ZIO.none
  }

  /** JobRunner stub that tracks which clubIds are submitted. */
  private def trackingRunner(submitted: Ref[List[Option[ClubId]]]): JobRunner = new JobRunner {
    override def submit(
      kind: JobKind,
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      submitted.update(clubId :: _).as(JobRunId.generate())

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
    override def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = ZIO.none
  }

  private def testPollFiberStopsOnScopeClose = test("poll fiber stops when enclosing scope closes") {
    val schedClubId = ClubId(300)
    for {
      pgClient    <- ZIO.service[PostgresClient]
      submissions <- Ref.make(0)
      runner = stubRunner(submissions)
      scheduler = new JobScheduler.JobSchedulerLive(runner, pgClient, pollInterval)

      // Always-due schedule: every poll triggers a submission.
      now <- Clock.instant
      _ <- Club.upsert(Club(schedClubId, now, ClubSlug("sched-test"), "Sched Test", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(schedClubId), None, intervalHours = 0, enabled = true, lastRunAt = None)
      )

      _ <- ZIO.scoped {
        for {
          _ <- scheduler.start
          _ <- TestClock.adjust(advanceWindow)
          _ <- submissions.get.repeatUntil(_ >= 2)
        } yield ()
      }

      // Scope closed → daemon interrupted. Further clock advancement must not produce polls.
      countAtClose <- submissions.get
      _ <- TestClock.adjust(advanceWindow)
      countAfter <- submissions.get
    } yield assertTrue(
      countAtClose >= 2,
      countAfter == countAtClose
    )
  }

  private def testDueScheduleSubmitted = test("submits job when schedule is due") {
    val dueClubId = ClubId(301)
    for {
      pgClient  <- ZIO.service[PostgresClient]
      submitted <- Ref.make(List.empty[Option[ClubId]])
      runner = trackingRunner(submitted)
      scheduler = new JobScheduler.JobSchedulerLive(runner, pgClient, pollInterval)

      now <- Clock.instant
      twoHoursAgo = now.minus(2, ChronoUnit.HOURS)
      _ <- Club.upsert(Club(dueClubId, twoHoursAgo, ClubSlug("due-test"), "Due Test", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(dueClubId), None, intervalHours = 1, enabled = true, lastRunAt = Some(twoHoursAgo))
      )
      _ <- ZIO.scoped {
        for {
          _ <- scheduler.start
          _ <- TestClock.adjust(advanceWindow)
          _ <- submitted.get.repeatUntil(_.exists(_.contains(dueClubId)))
        } yield ()
      }
    } yield assertCompletes
  }

  private def testNotYetDueScheduleSkipped = test("skips schedule that is not yet due") {
    val notDueClubId = ClubId(302)
    val ctrlClubId   = ClubId(312)
    for {
      pgClient  <- ZIO.service[PostgresClient]
      submitted <- Ref.make(List.empty[Option[ClubId]])
      runner = trackingRunner(submitted)
      scheduler = new JobScheduler.JobSchedulerLive(runner, pgClient, pollInterval)

      now <- Clock.instant
      _ <- Club.upsert(Club(notDueClubId, now, ClubSlug("notdue-test"), "Not Due Test", None, None, None))
      _ <- Club.upsert(Club(ctrlClubId, now, ClubSlug("notdue-ctrl"), "Ctrl Always-Due", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(notDueClubId), None, intervalHours = 24, enabled = true, lastRunAt = Some(now))
      )
      // Always-due control: its submission proves a full pollLoop iteration ran (and thus the
      // not-due schedule was evaluated and rejected). Originally introduced to replace a
      // `ZIO.sleep(200.millis)` race; now still useful as a sync signal under sequential execution.
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(ctrlClubId), None, intervalHours = 0, enabled = true, lastRunAt = None)
      )
      _ <- ZIO.scoped {
        for {
          _ <- scheduler.start
          _ <- TestClock.adjust(advanceWindow)
          _ <- submitted.get.repeatUntil(_.exists(_.contains(ctrlClubId)))
        } yield ()
      }
      clubs <- submitted.get
      notDueCount = clubs.count(_.contains(notDueClubId))
    } yield assertTrue(notDueCount == 0)
  }

  private def testDisabledScheduleSkipped = test("skips disabled schedule") {
    val disabledClubId = ClubId(303)
    val ctrlClubId     = ClubId(313)
    for {
      pgClient  <- ZIO.service[PostgresClient]
      submitted <- Ref.make(List.empty[Option[ClubId]])
      runner = trackingRunner(submitted)
      scheduler = new JobScheduler.JobSchedulerLive(runner, pgClient, pollInterval)

      now <- Clock.instant
      _ <- Club.upsert(Club(disabledClubId, now, ClubSlug("disabled-test"), "Disabled Test", None, None, None))
      _ <- Club.upsert(Club(ctrlClubId, now, ClubSlug("disabled-ctrl"), "Ctrl Always-Due", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(disabledClubId), None, intervalHours = 0, enabled = false, lastRunAt = None)
      )
      // Always-due control to anchor the assertion on a full pollLoop iteration completing.
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(ctrlClubId), None, intervalHours = 0, enabled = true, lastRunAt = None)
      )
      _ <- ZIO.scoped {
        for {
          _ <- scheduler.start
          _ <- TestClock.adjust(advanceWindow)
          _ <- submitted.get.repeatUntil(_.exists(_.contains(ctrlClubId)))
        } yield ()
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
          effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
        ): RIO[PostgresClient, JobRunId] =
          callCount.update(_ + 1) *> ZIO.fail(new RuntimeException("boom"))

        override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
        override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
        override def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = ZIO.none
      }
      scheduler = new JobScheduler.JobSchedulerLive(failingRunner, pgClient, pollInterval)

      now <- Clock.instant
      _ <- Club.upsert(Club(errorClubId, now, ClubSlug("error-test"), "Error Test", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule(0L, JobKind.Membership, Some(errorClubId), None, intervalHours = 0, enabled = true, lastRunAt = None)
      )
      _ <- ZIO.scoped {
        for {
          _ <- scheduler.start
          _ <- TestClock.adjust(advanceWindow)
          _ <- callCount.get.repeatUntil(_ >= 2)
        } yield ()
      }
    } yield assertCompletes // scheduler survived the error and polled again
  }

  private def testErrorInOneScheduleDoesNotBlockOthers =
    test("error in one schedule does not block other schedules") {
      val failClubId = ClubId(305)
      val goodClubId = ClubId(306)
      for {
        pgClient  <- ZIO.service[PostgresClient]
        submitted <- Ref.make(List.empty[Option[ClubId]])
        runner = new JobRunner {
          override def submit(
            kind: JobKind,
            clubId: Option[ClubId],
            params: Option[String],
            trigger: RunTrigger,
            effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
          ): RIO[PostgresClient, JobRunId] =
            submitted.update(clubId :: _) *>
              ZIO.when(clubId.contains(failClubId))(ZIO.fail(new RuntimeException("boom"))).as(JobRunId.generate())

          override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
          override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
          override def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = ZIO.none
        }
        scheduler = new JobScheduler.JobSchedulerLive(runner, pgClient, pollInterval)

        now <- Clock.instant
        _ <- Club.upsert(Club(failClubId, now, ClubSlug("fail-sched"), "Fail", None, None, None))
        _ <- Club.upsert(Club(goodClubId, now, ClubSlug("good-sched"), "Good", None, None, None))
        _ <- JobSchedule.insert(
          JobSchedule(0L, JobKind.Membership, Some(failClubId), None, intervalHours = 0, enabled = true, lastRunAt = None)
        )
        _ <- JobSchedule.insert(
          JobSchedule(0L, JobKind.History, Some(goodClubId), None, intervalHours = 0, enabled = true, lastRunAt = None)
        )
        _ <- ZIO.scoped {
          for {
            _ <- scheduler.start
            _ <- TestClock.adjust(advanceWindow)
            _ <- submitted.get.repeatUntil(_.exists(_.contains(goodClubId)))
          } yield ()
        }
      } yield assertCompletes
    }

  // Regression for #95: a forked job outliving its own intervalHours makes every subsequent tick
  // re-select the (still-due) schedule, `submit` conflicts, and the `*>` short-circuit leaves
  // `last_run_at` un-advanced. Contract: the scheduler keeps polling (does not crash) AND
  // `last_run_at` stays put — so the next tick after the job ends submits promptly. The conflict
  // is now logDebug, not logError; that log-level demotion is not asserted here (see plan: the
  // suite has no ZTestLogger and the daemon-fiber timing makes log capture brittle).
  private def testConflictKeepsPollingAndDoesNotAdvanceLastRunAt =
    test("running-job conflict keeps scheduler polling and does not advance last_run_at") {
      val conflictClubId = ClubId(307)
      for {
        pgClient  <- ZIO.service[PostgresClient]
        callCount <- Ref.make(0)
        conflictingRunner = new JobRunner {
          override def submit(
            kind: JobKind,
            clubId: Option[ClubId],
            params: Option[String],
            trigger: RunTrigger,
            effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
          ): RIO[PostgresClient, JobRunId] =
            callCount.update(_ + 1) *> ZIO.fail(ConflictException(s"A $kind job is already running"))

          override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
          override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
          override def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = ZIO.none
        }
        scheduler = new JobScheduler.JobSchedulerLive(conflictingRunner, pgClient, pollInterval)

        now <- Clock.instant
        twoHoursAgo = now.minus(2, ChronoUnit.HOURS)
        _ <- Club.upsert(Club(conflictClubId, twoHoursAgo, ClubSlug("conflict-test"), "Conflict Test", None, None, None))
        // Due (last run 2h ago, 1h interval). Each tick re-selects it; submit always conflicts.
        scheduleId <- JobSchedule.insert(
          JobSchedule(0L, JobKind.Membership, Some(conflictClubId), None, intervalHours = 1, enabled = true, lastRunAt = Some(twoHoursAgo))
        )
        _ <- ZIO.scoped {
          for {
            _ <- scheduler.start
            _ <- TestClock.adjust(advanceWindow)
            _ <- callCount.get.repeatUntil(_ >= 2)
          } yield ()
        }
        reloaded   <- JobSchedule.selectId(scheduleId)
        finalCount <- callCount.get
      } yield assertTrue(
        finalCount >= 2,                                   // kept polling across ticks despite the conflict
        reloaded.exists(_.lastRunAt.contains(twoHoursAgo)) // last_run_at NOT advanced by the conflict
      )
    }
}
