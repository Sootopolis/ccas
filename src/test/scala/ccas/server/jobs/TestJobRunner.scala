package ccas.server.jobs

import com.augustnagro.magnum.{sql, Transactor}
import zio.{durationInt, Scope, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.ServerTables
import ccas.utils.client.TestChessComClient
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.SqlZioTypes.connectZIO
import ccas.utils.CcasLogger

object TestJobRunner extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobRunner")(
    testSubmitSucceeds,
    testSubmitRecordsFailed,
    testSubmitRejectsDuplicate,
    testSubmitAllowsDifferentClub,
    testSubmitAllowsDifferentKind,
    testStatusUnknown,
    testRecentJobsOrdered
  ).provideShared(
    FreshSchemaLayer("test_job_runner", onInit = ServerTables.ensureTables),
    TestChessComClient.dummyLayer,
    JobRunner.live,
    Scope.default,
    CcasLogger.live(showProgress = false)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)

  private object Times {
    val t0: java.time.Instant = java.time.Instant.parse("2025-06-01T00:00:00Z")
  }

  private val clubIdA = ClubId(200)
  private val clubIdB = ClubId(201)
  private val clubIdC = ClubId(202)

  private val deleteAllJobRuns = for {
    _ <- connectZIO { val _ = sql"DELETE FROM job_run".update.run() }
    _ <- Club.upsert(Club(clubIdA, Times.t0, ClubSlug("club-a"), "Club A"))
    _ <- Club.upsert(Club(clubIdB, Times.t0, ClubSlug("club-b"), "Club B"))
    _ <- Club.upsert(Club(clubIdC, Times.t0, ClubSlug("club-c"), "Club C"))
    _ <- Club.upsert(Club(ClubId(203), Times.t0, ClubSlug("test-club"), "Test Club"))
    _ <- Club.upsert(Club(ClubId(204), Times.t0, ClubSlug("dup-club"), "Dup Club"))
  } yield ()

  private def awaitStatus(
    runner: JobRunner,
    id: JobRunId,
    maxWait: zio.Duration = 10.seconds
  ): ZIO[com.augustnagro.magnum.Transactor, Throwable, JobRun] =
    runner.status(id).flatMap {
      case Some(job) if job.status != JobRunStatus.Running => ZIO.succeed(job)
      case _                                               => ZIO.sleep(100.millis) *> awaitStatus(runner, id, maxWait)
    }.timeoutFail(new Exception(s"Job $id did not complete in time"))(maxWait)

  // --- Tests ---

  private def testSubmitSucceeds = test("submit succeeds and completes") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id     <- runner.submit(JobKind.Recruitment, Some(ClubId(203)), None, RunTrigger.Cli, _ => ZIO.unit)
      job    <- awaitStatus(runner, id)
    } yield assertTrue(
      job.status == JobRunStatus.Completed,
      job.completedAt.isDefined
    )
  }

  private def testSubmitRecordsFailed = test("submit records Failed on effect failure") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id     <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.fail(new Exception("boom")))
      job    <- awaitStatus(runner, id)
    } yield assertTrue(
      job.status == JobRunStatus.Failed,
      job.error.contains("boom")
    )
  }

  private def testSubmitRejectsDuplicate = test("submit rejects duplicate running job") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(ClubId(204)), None, RunTrigger.Cli, _ => ZIO.never)
      result <- runner.submit(JobKind.Recruitment, Some(ClubId(204)), None, RunTrigger.Cli, _ => ZIO.unit).either
    } yield assertTrue(
      result.isLeft,
      result.left.exists(_.isInstanceOf[JobConflictException])
    )
  }

  private def testSubmitAllowsDifferentClub = test("submit allows same kind with different club") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(clubIdA), None, RunTrigger.Cli, _ => ZIO.never)
      id2    <- runner.submit(JobKind.Recruitment, Some(clubIdB), None, RunTrigger.Cli, _ => ZIO.unit)
    } yield assertTrue(JobRunId.unwrap(id2).nonEmpty)
  }

  private def testSubmitAllowsDifferentKind = test("submit allows different kind with same club") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(clubIdC), None, RunTrigger.Cli, _ => ZIO.never)
      id2    <- runner.submit(JobKind.Membership, Some(clubIdC), None, RunTrigger.Cli, _ => ZIO.unit)
    } yield assertTrue(JobRunId.unwrap(id2).nonEmpty)
  }

  private def testStatusUnknown = test("status returns None for unknown id") {
    for {
      runner <- ZIO.service[JobRunner]
      result <- runner.status(JobRunId.wrap("nonexistent"))
    } yield assertTrue(result.isEmpty)
  }

  private def testRecentJobsOrdered = test("recentJobs returns ordered list") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id1    <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.unit)
      _      <- awaitStatus(runner, id1)
      id2    <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.unit)
      _      <- awaitStatus(runner, id2)
      // Wait briefly for any auto-follow-up MatchRef jobs to settle
      _      <- ZIO.sleep(500.millis)
      recent <- runner.recentJobs(50)
    } yield assertTrue(
      recent.size >= 2,
      recent.head.startedAt.isAfter(recent.last.startedAt) || recent.head.startedAt == recent.last.startedAt
    )
  }
}
