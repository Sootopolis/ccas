package ccas.server.jobs

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.ServerTables
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

object TestJobRunSql extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobRunSql")(
    testInsertAndSelectId,
    testSelectIdMissing,
    testUpdateStatusChangesFields,
    testSelectRunningForUpdateByKindAndClub,
    testSelectRunningForUpdateNullClub,
    testSelectRunningForUpdateIgnoresNonRunning,
    testSelectRecentOrdering,
    testMarkOrphansAsFailed
  ).provideShared(
    FreshSchemaLayer("test_job_run", onInit = ServerTables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
  }

  private val id0 = JobRunId.wrap("test-id-0")
  private val id1 = JobRunId.wrap("test-id-1")
  private val id2 = JobRunId.wrap("test-id-2")

  private val clubIdA = ClubId(200)
  private val clubIdB = ClubId(201)

  private val run0 =
    JobRun(id0, JobKind.Recruitment, Some(clubIdA), RunTrigger.Cli, JobRunStatus.Running, None, Times.t0, None, None)
  private val run1 =
    JobRun(id1, JobKind.Membership, Some(clubIdA), RunTrigger.Cli, JobRunStatus.Running, None, Times.t1, None, None)
  private val run2 =
    JobRun(id2, JobKind.MatchRef, None, RunTrigger.Cli, JobRunStatus.Running, Some("params"), Times.t2, None, None)

  private val deleteAll = for {
    _ <- connectZIO { val _ = sql"DELETE FROM job_run".update.run() }
    _ <- Club.upsert(Club(clubIdA, Times.t0, ClubSlug("club-a"), "Club A", None, None))
    _ <- Club.upsert(Club(clubIdB, Times.t0, ClubSlug("club-b"), "Club B", None, None))
  } yield ()

  // --- Tests ---

  private def testInsertAndSelectId = test("insert and selectId") {
    for {
      _      <- deleteAll
      _      <- JobRun.insert(run0)
      result <- JobRun.selectId(id0)
    } yield assertTrue(
      result.isDefined,
      result.get.id == id0,
      result.get.kind == JobKind.Recruitment,
      result.get.status == JobRunStatus.Running,
      result.get.clubId.contains(clubIdA),
      result.get.params.isEmpty,
      result.get.completedAt.isEmpty,
      result.get.error.isEmpty
    )
  }

  private def testSelectIdMissing = test("selectId returns None for missing id") {
    for {
      result <- JobRun.selectId(JobRunId.wrap("nonexistent"))
    } yield assertTrue(result.isEmpty)
  }

  private def testUpdateStatusChangesFields =
    test("updateStatus changes status, completedAt, error without touching other fields") {
      for {
        _      <- deleteAll
        _      <- JobRun.insert(run2) // has params = Some("params"), startedAt = Times.t2
        _      <- JobRun.updateStatus(id2, JobRunStatus.Completed, Some(Times.t1), None)
        result <- JobRun.selectId(id2)
      } yield assertTrue(
        result.get.status == JobRunStatus.Completed,
        result.get.completedAt.contains(Times.t1),
        result.get.startedAt == Times.t2,
        result.get.params.contains("params"),
        result.get.error.isEmpty
      )
    }

  private def testSelectRunningForUpdateByKindAndClub =
    test("selectRunningForUpdate finds by kind + clubId (Some)") {
      for {
        _           <- deleteAll
        _           <- JobRun.insert(run0) // Recruitment, club-a
        _           <- JobRun.insert(run1) // Membership, club-a
        recruitment <- JobRun.selectRunningForUpdate(JobKind.Recruitment, Some(clubIdA))
        membership  <- JobRun.selectRunningForUpdate(JobKind.Membership, Some(clubIdA))
      } yield assertTrue(
        recruitment.get.id == id0,
        membership.get.id == id1
      )
    }

  private def testSelectRunningForUpdateNullClub =
    test("selectRunningForUpdate finds by kind when clubId is None") {
      for {
        _        <- deleteAll
        _        <- JobRun.insert(run2) // MatchRef, None
        found    <- JobRun.selectRunningForUpdate(JobKind.MatchRef, None)
        notFound <- JobRun.selectRunningForUpdate(JobKind.MatchRef, Some(clubIdB))
      } yield assertTrue(
        found.get.id == id2,
        notFound.isEmpty
      )
    }

  private def testSelectRunningForUpdateIgnoresNonRunning =
    test("selectRunningForUpdate ignores non-Running rows") {
      val completed = JobRun(
        id0,
        JobKind.Recruitment,
        Some(clubIdA),
        RunTrigger.Cli,
        JobRunStatus.Completed,
        None,
        Times.t0,
        Some(Times.t1),
        None
      )
      val failed = JobRun(
        id1,
        JobKind.Recruitment,
        Some(clubIdA),
        RunTrigger.Cli,
        JobRunStatus.Failed,
        None,
        Times.t0,
        Some(Times.t1),
        Some("err")
      )
      for {
        _      <- deleteAll
        _      <- JobRun.insert(completed)
        _      <- JobRun.insert(failed)
        result <- JobRun.selectRunningForUpdate(JobKind.Recruitment, Some(clubIdA))
      } yield assertTrue(result.isEmpty)
    }

  private def testSelectRecentOrdering = test("selectRecent orders by startedAt DESC with limit") {
    for {
      _      <- deleteAll
      _      <- JobRun.insert(run0.copy(startedAt = Times.t0))
      _      <- JobRun.insert(run1.copy(startedAt = Times.t1))
      _      <- JobRun.insert(run2.copy(startedAt = Times.t2))
      recent <- JobRun.selectRecent(2)
    } yield assertTrue(
      recent.size == 2,
      recent.head.id == id2,
      recent(1).id == id1
    )
  }

  private def testMarkOrphansAsFailed = test("markOrphansAsFailed marks Running → Failed") {
    val running1 =
      JobRun(id0, JobKind.Recruitment, None, RunTrigger.Cli, JobRunStatus.Running, None, Times.t0, None, None)
    val running2 =
      JobRun(id1, JobKind.Membership, None, RunTrigger.Cli, JobRunStatus.Running, None, Times.t1, None, None)
    val completed =
      JobRun(id2, JobKind.MatchRef, None, RunTrigger.Cli, JobRunStatus.Completed, None, Times.t2, Some(Times.t2), None)
    for {
      _     <- deleteAll
      _     <- JobRun.insert(running1)
      _     <- JobRun.insert(running2)
      _     <- JobRun.insert(completed)
      count <- JobRun.markOrphansAsFailed
      r0    <- JobRun.selectId(id0)
      r1    <- JobRun.selectId(id1)
      r2    <- JobRun.selectId(id2)
    } yield assertTrue(
      count == 2,
      r0.get.status == JobRunStatus.Failed,
      r0.get.error.contains("Service restarted"),
      r1.get.status == JobRunStatus.Failed,
      r2.get.status == JobRunStatus.Completed
    )
  }
}
