package ccas.server.scheduler

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.Club
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.jobs.JobKind
import ccas.server.ServerTables
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

object TestJobScheduleSql extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobScheduleSql")(
    testInsertAndSelectId,
    testSelectAll,
    testSelectEnabledFiltersDisabled,
    testUpdateLastRunAt,
    testUpdateAllFields,
    testUpdatePartialFields,
    testUpdateNoFields,
    testUpdateSetParamsToNull,
    testDelete,
    testUniqueConstraint
  ).provideShared(
    FreshSchemaLayer("test_job_schedule", onInit = ServerTables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
  }

  private val clubIdA = ClubId(200)
  private val clubIdB = ClubId(201)

  private val deleteAll = for {
    _ <- connectZIO { val _ = sql"DELETE FROM job_schedule".update.run() }
    _ <- Club.upsert(Club(clubIdA, Times.t0, ClubSlug("club-a"), "Club A"))
    _ <- Club.upsert(Club(clubIdB, Times.t0, ClubSlug("club-b"), "Club B"))
  } yield ()

  // --- Tests ---

  private def testInsertAndSelectId = test("insert returns generated id and selectId retrieves") {
    val schedule =
      JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), Some("p=1"), 24, enabled = true, None)
    for {
      _      <- deleteAll
      id     <- JobSchedule.insert(schedule)
      result <- JobSchedule.selectId(id)
    } yield assertTrue(
      id > 0L,
      result.isDefined,
      result.get.id == id,
      result.get.kind == JobKind.Recruitment,
      result.get.clubId.contains(clubIdA),
      result.get.params.contains("p=1"),
      result.get.intervalHours == 24,
      result.get.enabled,
      result.get.lastRunAt.isEmpty
    )
  }

  private def testSelectAll = test("selectAll returns all schedules") {
    val s1 = JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), None, 12, enabled = true, None)
    val s2 = JobSchedule(0L, JobKind.Membership, Some(clubIdB), None, 24, enabled = true, None)
    for {
      _   <- deleteAll
      _   <- JobSchedule.insert(s1)
      _   <- JobSchedule.insert(s2)
      all <- JobSchedule.selectAll
    } yield assertTrue(all.size == 2)
  }

  private def testSelectEnabledFiltersDisabled = test("selectEnabled filters disabled") {
    val enabled  = JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), None, 12, enabled = true, None)
    val disabled = JobSchedule(0L, JobKind.Membership, Some(clubIdB), None, 24, enabled = false, None)
    for {
      _      <- deleteAll
      _      <- JobSchedule.insert(enabled)
      _      <- JobSchedule.insert(disabled)
      result <- JobSchedule.selectEnabled
    } yield assertTrue(
      result.size == 1,
      result.head.kind == JobKind.Recruitment
    )
  }

  private def testUpdateLastRunAt = test("updateLastRunAt sets timestamp") {
    val schedule = JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), None, 12, enabled = true, None)
    for {
      _      <- deleteAll
      id     <- JobSchedule.insert(schedule)
      _      <- JobSchedule.updateLastRunAt(id, Times.t0)
      result <- JobSchedule.selectId(id)
    } yield assertTrue(result.get.lastRunAt.contains(Times.t0))
  }

  private def testUpdateAllFields = test("update with all optional fields") {
    val schedule = JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), None, 12, enabled = true, None)
    for {
      _      <- deleteAll
      id     <- JobSchedule.insert(schedule)
      _      <- JobSchedule.update(id, Some(48), Some(false), Some(Some("new-params")))
      result <- JobSchedule.selectId(id)
    } yield assertTrue(
      result.get.intervalHours == 48,
      !result.get.enabled,
      result.get.params.contains("new-params")
    )
  }

  private def testUpdatePartialFields = test("update with partial fields") {
    val schedule =
      JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), Some("original"), 12, enabled = true, None)
    for {
      _      <- deleteAll
      id     <- JobSchedule.insert(schedule)
      _      <- JobSchedule.update(id, Some(48), None, None)
      result <- JobSchedule.selectId(id)
    } yield assertTrue(
      result.get.intervalHours == 48,
      result.get.enabled,
      result.get.params.contains("original")
    )
  }

  private def testUpdateNoFields = test("update with no fields is a no-op") {
    val schedule = JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), None, 12, enabled = true, None)
    for {
      _      <- deleteAll
      id     <- JobSchedule.insert(schedule)
      _      <- JobSchedule.update(id, None, None, None)
      result <- JobSchedule.selectId(id)
    } yield assertTrue(
      result.get.intervalHours == 12,
      result.get.enabled
    )
  }

  private def testUpdateSetParamsToNull = test("update can set params to null via Some(None)") {
    val schedule =
      JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), Some("original"), 12, enabled = true, None)
    for {
      _      <- deleteAll
      id     <- JobSchedule.insert(schedule)
      _      <- JobSchedule.update(id, None, None, Some(None))
      result <- JobSchedule.selectId(id)
    } yield assertTrue(
      result.get.params.isEmpty,
      result.get.intervalHours == 12,
      result.get.enabled
    )
  }

  private def testDelete = test("delete removes the schedule") {
    val schedule = JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), None, 12, enabled = true, None)
    for {
      _      <- deleteAll
      id     <- JobSchedule.insert(schedule)
      _      <- JobSchedule.delete(id)
      result <- JobSchedule.selectId(id)
    } yield assertTrue(result.isEmpty)
  }

  private def testUniqueConstraint = test("unique constraint on (kind, club_id)") {
    val s1 = JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), None, 12, enabled = true, None)
    val s2 = JobSchedule(0L, JobKind.Recruitment, Some(clubIdA), None, 24, enabled = false, None)
    for {
      _      <- deleteAll
      _      <- JobSchedule.insert(s1)
      result <- JobSchedule.insert(s2).exit
    } yield assertTrue(!result.isSuccess)
  }
}
