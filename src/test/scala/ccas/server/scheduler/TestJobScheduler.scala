package ccas.server.scheduler

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.{durationInt, Clock, Duration, Ref, RIO, UIO, URIO, ZIO}
import zio.stream.ZStream
import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, TestClock, ZIOSpecDefault}

import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.ServerTables
import ccas.server.jobs.{JobKind, JobLogs, JobRun, JobRunner}
import ccas.utils.ProgressDisplay
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ConflictException
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient, TestDbCleanup}

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
      testCronScheduleSubmitted,
      testNotYetDueScheduleSkipped,
      testDisabledScheduleSkipped,
      testErrorDoesNotCrashScheduler,
      testErrorInOneScheduleDoesNotBlockOthers,
      testConflictKeepsPollingAndDoesNotAdvanceLastRunAt,
      testParamsThreadedToSubmit,
      testMalformedParamsDoesNotBlockOthers,
      testIdleSchedulerReadsOnlyWhenDue,
      testRecheckMakesTheNextTickRead,
      testUnseenWriteIsReadWithinTheGap,
      testUndecodableParamsDoNotReadEveryTick
    )
      @@ TestAspect.before(TestDbCleanup.clearJobSchedules)
      @@ TestAspect.sequential
      @@ TestAspect.timeout(30.seconds)
  ).provideShared(
    FreshSchemaLayer(schema = "test_scheduler", onInit = ServerTables.ensureTables)
  )

  // Virtual poll interval driven by TestClock; no wall-clock dependency. Each test advances by
  // `advanceWindow` in one go, relying on `Schedule.fixed`'s catch-up behavior to fire overdue
  // iterations back-to-back. A single `pollInterval` advance would suffice now that tests run
  // sequentially against a clean `job_schedule`; the 5× window stays as defensive headroom (it lets
  // `testErrorDoesNotCrashScheduler` watch `callCount` cross 2 across distinct iterations).
  private val pollInterval: Duration  = 1.minute
  private val advanceWindow: Duration = pollInterval.multipliedBy(5)
  private val maxReadGap: Duration    = 1.day

  private def makeScheduler(runner: JobRunner, pgClient: PostgresClient): UIO[JobScheduler.JobSchedulerLive] =
    JobScheduler.JobSchedulerLive.make(
      runner,
      pgClient,
      pollInterval = pollInterval,
      maxReadGap = maxReadGap,
      readSchedules = JobSchedule.selectEnabled
    )

  /** JobRunner stub that counts submissions without running effects. */
  private def stubRunner(submissions: Ref[Int]): JobRunner = new JobRunner {
    override def submit(
      kind: JobKind,
      clubIdOption: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      submissions.update(_ + 1).as(JobRunId.generate())

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
    override def cancel(id: JobRunId): UIO[Boolean]                        = ZIO.succeed(false)
    override def interruptAll: UIO[Unit]                                   = ZIO.unit
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
    override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs]     = ZIO.succeed(JobLogs.NoSuchJob)
    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = ZIO.none
    override def sweepLogs: URIO[PostgresClient, Int] = ZIO.succeed(0)
  }

  /** JobRunner stub that captures the raw `params` string of each submission. */
  private def paramsCapturingRunner(captured: Ref[List[Option[String]]]): JobRunner = new JobRunner {
    override def submit(
      kind: JobKind,
      clubIdOption: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      captured.update(params :: _).as(JobRunId.generate())

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
    override def cancel(id: JobRunId): UIO[Boolean]                        = ZIO.succeed(false)
    override def interruptAll: UIO[Unit]                                   = ZIO.unit
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
    override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs]     = ZIO.succeed(JobLogs.NoSuchJob)
    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = ZIO.none
    override def sweepLogs: URIO[PostgresClient, Int] = ZIO.succeed(0)
  }

  /** JobRunner stub that tracks which clubIds are submitted. */
  private def trackingRunner(submitted: Ref[List[Option[ClubId]]]): JobRunner = new JobRunner {
    override def submit(
      kind: JobKind,
      clubIdOption: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      submitted.update(clubIdOption :: _).as(JobRunId.generate())

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
    override def cancel(id: JobRunId): UIO[Boolean]                        = ZIO.succeed(false)
    override def interruptAll: UIO[Unit]                                   = ZIO.unit
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
    override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs]     = ZIO.succeed(JobLogs.NoSuchJob)
    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = ZIO.none
    override def sweepLogs: URIO[PostgresClient, Int] = ZIO.succeed(0)
  }

  private def testPollFiberStopsOnScopeClose = test("poll fiber stops when enclosing scope closes") {
    val schedClubId = ClubId(300)
    for {
      pgClient    <- ZIO.service[PostgresClient]
      submissions <- Ref.make(0)
      runner = stubRunner(submissions)
      scheduler <- makeScheduler(runner, pgClient)

      // Always-due schedule: every poll triggers a submission.
      now <- Clock.instant
      _   <- Club.upsert(Club(schedClubId, now, ClubSlug("sched-test"), "Sched Test", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule.interval(
          id = 0L,
          kind = JobKind.Membership,
          clubIdOption = Some(schedClubId),
          params = None,
          intervalHours = 0,
          enabled = true,
          lastRunAt = None
        )
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
      _            <- TestClock.adjust(advanceWindow)
      countAfter   <- submissions.get
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
      scheduler <- makeScheduler(runner, pgClient)

      now <- Clock.instant
      twoHoursAgo = now.minus(2, ChronoUnit.HOURS)
      _ <- Club.upsert(Club(dueClubId, twoHoursAgo, ClubSlug("due-test"), "Due Test", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule.interval(
          id = 0L,
          kind = JobKind.Membership,
          clubIdOption = Some(dueClubId),
          params = None,
          intervalHours = 1,
          enabled = true,
          lastRunAt = Some(twoHoursAgo)
        )
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

  private def testCronScheduleSubmitted = test("submits job when a cron schedule's boundary has passed") {
    val cronClubId = ClubId(308)
    for {
      pgClient  <- ZIO.service[PostgresClient]
      submitted <- Ref.make(List.empty[Option[ClubId]])
      runner = trackingRunner(submitted)
      scheduler <- makeScheduler(runner, pgClient)

      now <- Clock.instant
      _   <- Club.upsert(Club(cronClubId, now, ClubSlug("cron-test"), "Cron Test", None, None, None))
      // Fires every minute (normalized 6-field, dow blanked to `?`); CatchUp sidesteps grace timing under the
      // virtual clock. lastRunAt = now ⇒ the first boundary crossed by advancing the clock is due.
      _ <- JobSchedule.insert(
        JobSchedule.cron(
          id = 0L,
          kind = JobKind.Membership,
          clubIdOption = Some(cronClubId),
          params = None,
          cronExpr = "0 * * * * ?",
          timezone = "UTC",
          misfire = MisfirePolicy.CatchUp,
          enabled = true,
          lastRunAt = Some(now)
        )
      )
      _ <- ZIO.scoped {
        for {
          _ <- scheduler.start
          _ <- TestClock.adjust(advanceWindow)
          _ <- submitted.get.repeatUntil(_.exists(_.contains(cronClubId)))
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
      scheduler <- makeScheduler(runner, pgClient)

      now <- Clock.instant
      _   <- Club.upsert(Club(notDueClubId, now, ClubSlug("notdue-test"), "Not Due Test", None, None, None))
      _   <- Club.upsert(Club(ctrlClubId, now, ClubSlug("notdue-ctrl"), "Ctrl Always-Due", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule.interval(
          id = 0L,
          kind = JobKind.Membership,
          clubIdOption = Some(notDueClubId),
          params = None,
          intervalHours = 24,
          enabled = true,
          lastRunAt = Some(now)
        )
      )
      // Always-due control: its submission proves a full pollLoop iteration ran (and thus the
      // not-due schedule was evaluated and rejected). Originally introduced to replace a
      // `ZIO.sleep(200.millis)` race; now still useful as a sync signal under sequential execution.
      _ <- JobSchedule.insert(
        JobSchedule.interval(
          id = 0L,
          kind = JobKind.Membership,
          clubIdOption = Some(ctrlClubId),
          params = None,
          intervalHours = 0,
          enabled = true,
          lastRunAt = None
        )
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
      scheduler <- makeScheduler(runner, pgClient)

      now <- Clock.instant
      _   <- Club.upsert(Club(disabledClubId, now, ClubSlug("disabled-test"), "Disabled Test", None, None, None))
      _   <- Club.upsert(Club(ctrlClubId, now, ClubSlug("disabled-ctrl"), "Ctrl Always-Due", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule.interval(
          id = 0L,
          kind = JobKind.Membership,
          clubIdOption = Some(disabledClubId),
          params = None,
          intervalHours = 0,
          enabled = false,
          lastRunAt = None
        )
      )
      // Always-due control to anchor the assertion on a full pollLoop iteration completing.
      _ <- JobSchedule.insert(
        JobSchedule.interval(
          id = 0L,
          kind = JobKind.Membership,
          clubIdOption = Some(ctrlClubId),
          params = None,
          intervalHours = 0,
          enabled = true,
          lastRunAt = None
        )
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
      pgClient  <- ZIO.service[PostgresClient]
      callCount <- Ref.make(0)
      failingRunner = new JobRunner {
        override def submit(
          kind: JobKind,
          clubIdOption: Option[ClubId],
          params: Option[String],
          trigger: RunTrigger,
          effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
        ): RIO[PostgresClient, JobRunId] =
          callCount.update(_ + 1) *> ZIO.fail(new RuntimeException("boom"))

        override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
        override def cancel(id: JobRunId): UIO[Boolean]                        = ZIO.succeed(false)
        override def interruptAll: UIO[Unit]                                   = ZIO.unit
        override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
        override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs]     = ZIO.succeed(JobLogs.NoSuchJob)
        override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
          ZIO.none
        override def sweepLogs: URIO[PostgresClient, Int] = ZIO.succeed(0)
      }
      scheduler <- makeScheduler(failingRunner, pgClient)

      now <- Clock.instant
      _   <- Club.upsert(Club(errorClubId, now, ClubSlug("error-test"), "Error Test", None, None, None))
      _ <- JobSchedule.insert(
        JobSchedule.interval(
          id = 0L,
          kind = JobKind.Membership,
          clubIdOption = Some(errorClubId),
          params = None,
          intervalHours = 0,
          enabled = true,
          lastRunAt = None
        )
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
            clubIdOption: Option[ClubId],
            params: Option[String],
            trigger: RunTrigger,
            effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
          ): RIO[PostgresClient, JobRunId] =
            submitted.update(clubIdOption :: _) *>
              ZIO.when(clubIdOption.contains(failClubId))(ZIO.fail(new RuntimeException("boom"))).as(
                JobRunId.generate()
              )

          override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
          override def cancel(id: JobRunId): UIO[Boolean]                        = ZIO.succeed(false)
          override def interruptAll: UIO[Unit]                                   = ZIO.unit
          override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
          override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs]     = ZIO.succeed(JobLogs.NoSuchJob)
          override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
            ZIO.none
          override def sweepLogs: URIO[PostgresClient, Int] = ZIO.succeed(0)
        }
        scheduler <- makeScheduler(runner, pgClient)

        now <- Clock.instant
        _   <- Club.upsert(Club(failClubId, now, ClubSlug("fail-sched"), "Fail", None, None, None))
        _   <- Club.upsert(Club(goodClubId, now, ClubSlug("good-sched"), "Good", None, None, None))
        _ <- JobSchedule.insert(
          JobSchedule.interval(
            id = 0L,
            kind = JobKind.Membership,
            clubIdOption = Some(failClubId),
            params = None,
            intervalHours = 0,
            enabled = true,
            lastRunAt = None
          )
        )
        _ <- JobSchedule.insert(
          JobSchedule.interval(
            id = 0L,
            kind = JobKind.History,
            clubIdOption = Some(goodClubId),
            params = None,
            intervalHours = 0,
            enabled = true,
            lastRunAt = None
          )
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
            clubIdOption: Option[ClubId],
            params: Option[String],
            trigger: RunTrigger,
            effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
          ): RIO[PostgresClient, JobRunId] =
            callCount.update(_ + 1) *> ZIO.fail(ConflictException(s"A $kind job is already running"))

          override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = ZIO.none
          override def cancel(id: JobRunId): UIO[Boolean]                        = ZIO.succeed(false)
          override def interruptAll: UIO[Unit]                                   = ZIO.unit
          override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = ZIO.succeed(Nil)
          override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs]     = ZIO.succeed(JobLogs.NoSuchJob)
          override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
            ZIO.none
          override def sweepLogs: URIO[PostgresClient, Int] = ZIO.succeed(0)
        }
        scheduler <- makeScheduler(conflictingRunner, pgClient)

        now <- Clock.instant
        twoHoursAgo = now.minus(2, ChronoUnit.HOURS)
        _ <- Club.upsert(
          Club(conflictClubId, twoHoursAgo, ClubSlug("conflict-test"), "Conflict Test", None, None, None)
        )
        // Due (last run 2h ago, 1h interval). Each tick re-selects it; submit always conflicts.
        scheduleId <- JobSchedule.insert(
          JobSchedule.interval(
            id = 0L,
            kind = JobKind.Membership,
            clubIdOption = Some(conflictClubId),
            params = None,
            intervalHours = 1,
            enabled = true,
            lastRunAt = Some(twoHoursAgo)
          )
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

  // A well-formed `params` string is decoded (proving the scheduler now consumes it) and the raw string still
  // reaches `submit` for persistence into `job_run.params`. ClubData is a global (clubId = None) kind, so this
  // needs no club fixture or effect execution.
  private def testParamsThreadedToSubmit =
    test("well-formed schedule params decode and the raw string reaches submit") {
      val paramsJson = """{"minAgeHours":24}"""
      for {
        pgClient <- ZIO.service[PostgresClient]
        captured <- Ref.make(List.empty[Option[String]])
        runner = paramsCapturingRunner(captured)
        scheduler <- makeScheduler(runner, pgClient)
        _ <- JobSchedule.insert(
          JobSchedule.interval(
            id = 0L,
            kind = JobKind.ClubData,
            clubIdOption = None,
            params = Some(paramsJson),
            intervalHours = 0,
            enabled = true,
            lastRunAt = None
          )
        )
        _ <- ZIO.scoped {
          for {
            _ <- scheduler.start
            _ <- TestClock.adjust(advanceWindow)
            _ <- captured.get.repeatUntil(_.nonEmpty)
          } yield ()
        }
        seen <- captured.get
      } yield assertTrue(seen.contains(Some(paramsJson)))
    }

  // A schedule whose `params` is malformed JSON fails to decode *before* `submit`; the poll loop's per-schedule
  // guard isolates it (logs, does not crash) so an always-due control on another schedule still submits, and the
  // malformed row itself never submits.
  private def testMalformedParamsDoesNotBlockOthers =
    test("malformed schedule params do not block other schedules") {
      val ctrlClubId = ClubId(314)
      for {
        pgClient  <- ZIO.service[PostgresClient]
        submitted <- Ref.make(List.empty[Option[ClubId]])
        runner = trackingRunner(submitted)
        scheduler <- makeScheduler(runner, pgClient)

        now <- Clock.instant
        _   <- Club.upsert(Club(ctrlClubId, now, ClubSlug("badparams-ctrl"), "Ctrl Always-Due", None, None, None))
        // Malformed-params global job: decode fails before submit and must not block the control.
        _ <- JobSchedule.insert(
          JobSchedule.interval(
            id = 0L,
            kind = JobKind.ClubData,
            clubIdOption = None,
            params = Some("{not-json"),
            intervalHours = 0,
            enabled = true,
            lastRunAt = None
          )
        )
        _ <- JobSchedule.insert(
          JobSchedule.interval(
            id = 0L,
            kind = JobKind.Membership,
            clubIdOption = Some(ctrlClubId),
            params = None,
            intervalHours = 0,
            enabled = true,
            lastRunAt = None
          )
        )
        _ <- ZIO.scoped {
          for {
            _ <- scheduler.start
            _ <- TestClock.adjust(advanceWindow)
            _ <- submitted.get.repeatUntil(_.exists(_.contains(ctrlClubId)))
          } yield ()
        }
        clubs <- submitted.get
      } yield assertTrue(
        clubs.exists(_.contains(ctrlClubId)), // control still submitted
        !clubs.contains(None)                 // malformed ClubData row never submitted
      )
    }

  /** A scheduler whose reads of the schedules are counted and whose next-read time the test can watch, so a test can
    * wait for a read to finish before it moves the clock: otherwise a tick could find no next-read time yet and read.
    */
  private final case class Watched(scheduler: JobScheduler.JobSchedulerLive, reads: Ref[Int]) {
    def readSettled(count: Int): UIO[Unit] =
      (reads.get <*> scheduler.nextRead.get).repeatUntil((n, next) => n >= count && next.isDefined).unit
  }

  private def watched(runner: JobRunner, gap: Duration): URIO[PostgresClient, Watched] =
    for {
      pgClient <- ZIO.service[PostgresClient]
      reads    <- Ref.make(0)
      scheduler <- JobScheduler.JobSchedulerLive.make(
        runner,
        pgClient,
        pollInterval = pollInterval,
        maxReadGap = gap,
        readSchedules = JobSchedule.selectEnabled <* reads.update(_ + 1)
      )
    } yield Watched(scheduler, reads)

  private def membershipEvery(clubId: ClubId, hours: Short, lastRunAtOption: Option[Instant]): JobSchedule =
    JobSchedule.interval(
      id = 0L,
      kind = JobKind.Membership,
      clubIdOption = Some(clubId),
      params = None,
      intervalHours = hours,
      enabled = true,
      lastRunAt = lastRunAtOption
    )

  /** #422: between due times the ticks stay in the JVM, so an idle server lets the database suspend. */
  private def testIdleSchedulerReadsOnlyWhenDue =
    test("an idle scheduler does not read the schedules again until one is due, then submits it at that tick") {
      val clubId = ClubId(315)
      for {
        submitted <- Ref.make(List.empty[Option[ClubId]])
        w         <- watched(trackingRunner(submitted), maxReadGap)
        now       <- Clock.instant
        _         <- Club.upsert(Club(clubId, now, ClubSlug("idle-test"), "Idle Test", None, None, None))
        _         <- JobSchedule.insert(membershipEvery(clubId, hours = 1, Some(now)))
        result <- ZIO.scoped {
          for {
            _           <- w.scheduler.start
            _           <- w.readSettled(1)
            _           <- TestClock.adjust(59.minutes)
            readsBefore <- w.reads.get
            clubsBefore <- submitted.get
            _           <- TestClock.adjust(1.minute)
            clubsAtDue  <- submitted.get.repeatUntil(_.nonEmpty)
            readsAtDue  <- w.reads.get
          } yield (readsBefore, clubsBefore, clubsAtDue, readsAtDue)
        }
        (readsBefore, clubsBefore, clubsAtDue, readsAtDue) = result
      } yield assertTrue(
        readsBefore == 1,
        clubsBefore.isEmpty,
        clubsAtDue == List(Some(clubId)),
        readsAtDue == 2
      )
    }

  private def testRecheckMakesTheNextTickRead =
    test(
      "a schedule written behind the scheduler's back waits for a recheck, then the next tick reads and submits it"
    ) {
      val clubId = ClubId(316)
      for {
        submitted <- Ref.make(List.empty[Option[ClubId]])
        w         <- watched(trackingRunner(submitted), maxReadGap)
        now       <- Clock.instant
        _         <- Club.upsert(Club(clubId, now, ClubSlug("recheck-test"), "Recheck Test", None, None, None))
        result <- ZIO.scoped {
          for {
            _           <- w.scheduler.start
            _           <- w.readSettled(1)
            _           <- JobSchedule.insert(membershipEvery(clubId, hours = 24, None))
            _           <- TestClock.adjust(advanceWindow)
            readsBefore <- w.reads.get
            _           <- w.scheduler.recheck
            _           <- TestClock.adjust(pollInterval)
            clubs       <- submitted.get.repeatUntil(_.nonEmpty)
          } yield (readsBefore, clubs)
        }
        (readsBefore, clubs) = result
      } yield assertTrue(readsBefore == 1, clubs == List(Some(clubId)))
    }

  /** Hand-edited SQL, or a second server, changes the schedules without a recheck; the gap bounds how late it's seen. */
  private def testUnseenWriteIsReadWithinTheGap =
    test("without a recheck, a schedule written behind the scheduler's back is read once the max read gap has passed") {
      val clubId = ClubId(317)
      val gap    = 10.minutes
      for {
        submitted <- Ref.make(List.empty[Option[ClubId]])
        w         <- watched(trackingRunner(submitted), gap)
        now       <- Clock.instant
        _         <- Club.upsert(Club(clubId, now, ClubSlug("gap-test"), "Gap Test", None, None, None))
        result <- ZIO.scoped {
          for {
            _           <- w.scheduler.start
            _           <- w.readSettled(1)
            _           <- JobSchedule.insert(membershipEvery(clubId, hours = 24, None))
            _           <- TestClock.adjust(gap.minus(pollInterval))
            clubsBefore <- submitted.get
            _           <- TestClock.adjust(pollInterval)
            clubs       <- submitted.get.repeatUntil(_.nonEmpty)
          } yield (clubsBefore, clubs)
        }
        (clubsBefore, clubs) = result
      } yield assertTrue(clubsBefore.isEmpty, clubs == List(Some(clubId)))
    }

  /** A row the API accepted with bad params can't fire until it is changed, which asks for a recheck. */
  private def testUndecodableParamsDoNotReadEveryTick =
    test("a due schedule whose params won't decode does not make every tick read the schedules") {
      for {
        submitted <- Ref.make(0)
        w         <- watched(stubRunner(submitted), maxReadGap)
        _ <- JobSchedule.insert(
          JobSchedule.interval(
            id = 0L,
            kind = JobKind.ClubData,
            clubIdOption = None,
            params = Some("{not-json"),
            intervalHours = 24,
            enabled = true,
            lastRunAt = None
          )
        )
        readsAfter <- ZIO.scoped {
          for {
            _     <- w.scheduler.start
            _     <- w.readSettled(1)
            _     <- TestClock.adjust(advanceWindow)
            reads <- w.reads.get
          } yield reads
        }
        count <- submitted.get
      } yield assertTrue(readsAfter == 1, count == 0)
    }
}
