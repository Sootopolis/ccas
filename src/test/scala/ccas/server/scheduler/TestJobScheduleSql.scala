package ccas.server.scheduler

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, ManagedClub}
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
    testUniqueConstraint,
    testSeedGlobalInsertsRow,
    testSeedGlobalIdempotent,
    testSeedGlobalPreservesExisting,
    testSeedPerClubInsertsRow,
    testSeedPerClubIdempotent,
    testSeedPerClubDistinctClubs,
    testSeedPerClubRespectsEnabled,
    testSeedPerClubPreservesExisting,
    testEnsureTablesSeedsManagedOnly,
    testDeleteByClubRemovesAllKinds,
    testDeleteByClubLeavesOtherClubs,
    testDeleteByClubLeavesGlobalRows
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
    _ <- connectZIO { val _ = sql"DELETE FROM managed_club".update.run() }
    _ <- connectZIO { val _ = sql"DELETE FROM job_schedule".update.run() }
    _ <- Club.upsert(Club(clubIdA, Times.t0, ClubSlug("club-a"), "Club A", None, None, None))
    _ <- Club.upsert(Club(clubIdB, Times.t0, ClubSlug("club-b"), "Club B", None, None, None))
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

  private def globalRows(kind: JobKind) =
    JobSchedule.selectAll.map(_.filter(s => s.kind == kind && s.clubId.isEmpty))

  private def testSeedGlobalInsertsRow = test("seedGlobalIfAbsent inserts a global (club_id NULL) row") {
    for {
      _        <- deleteAll
      inserted <- JobSchedule.seedGlobalIfAbsent(ScheduleSeed(JobKind.MatchRef, 24, enabled = true))
      rows     <- globalRows(JobKind.MatchRef)
    } yield assertTrue(
      inserted == 1,
      rows.size == 1,
      rows.head.clubId.isEmpty,
      rows.head.intervalHours == 24,
      rows.head.enabled,
      rows.head.lastRunAt.isEmpty
    )
  }

  private def testSeedGlobalIdempotent = test("seedGlobalIfAbsent is a no-op on re-seed") {
    for {
      _       <- deleteAll
      first   <- JobSchedule.seedGlobalIfAbsent(ScheduleSeed(JobKind.ClubData, 6, enabled = true))
      second  <- JobSchedule.seedGlobalIfAbsent(ScheduleSeed(JobKind.ClubData, 6, enabled = true))
      rows    <- globalRows(JobKind.ClubData)
    } yield assertTrue(first == 1, second == 0, rows.size == 1)
  }

  private def testSeedGlobalPreservesExisting =
    test("seedGlobalIfAbsent leaves a hand-disabled global row untouched") {
      val existing = JobSchedule(0L, JobKind.MatchRef, None, None, 99, enabled = false, None)
      for {
        _    <- deleteAll
        _    <- JobSchedule.insert(existing)
        n    <- JobSchedule.seedGlobalIfAbsent(ScheduleSeed(JobKind.MatchRef, 24, enabled = true))
        rows <- globalRows(JobKind.MatchRef)
      } yield assertTrue(
        n == 0,
        rows.size == 1,
        rows.head.intervalHours == 99,
        !rows.head.enabled
      )
    }

  private def perClubRows(kind: JobKind, clubId: ClubId) =
    JobSchedule.selectAll.map(_.filter(s => s.kind == kind && s.clubId.contains(clubId)))

  private def testSeedPerClubInsertsRow = test("seedPerClubIfAbsent inserts a per-club (club_id non-NULL) row") {
    for {
      _        <- deleteAll
      inserted <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = true))
      rows     <- perClubRows(JobKind.History, clubIdA)
    } yield assertTrue(
      inserted == 1,
      rows.size == 1,
      rows.head.clubId.contains(clubIdA),
      rows.head.intervalHours == 24,
      rows.head.enabled,
      rows.head.params.isEmpty,
      rows.head.lastRunAt.isEmpty
    )
  }

  private def testSeedPerClubIdempotent = test("seedPerClubIfAbsent is a no-op on re-seed") {
    for {
      _      <- deleteAll
      first  <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = true))
      second <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = true))
      rows   <- perClubRows(JobKind.History, clubIdA)
    } yield assertTrue(first == 1, second == 0, rows.size == 1)
  }

  private def testSeedPerClubDistinctClubs = test("seedPerClubIfAbsent seeds one row per club, not globally") {
    for {
      _  <- deleteAll
      a  <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = true))
      b  <- JobSchedule.seedPerClubIfAbsent(clubIdB, ScheduleSeed(JobKind.History, 24, enabled = true))
      ra <- perClubRows(JobKind.History, clubIdA)
      rb <- perClubRows(JobKind.History, clubIdB)
    } yield assertTrue(a == 1, b == 1, ra.size == 1, rb.size == 1)
  }

  private def testSeedPerClubRespectsEnabled = test("seedPerClubIfAbsent honours the enabled flag") {
    for {
      _    <- deleteAll
      _    <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = false))
      rows <- perClubRows(JobKind.History, clubIdA)
      en   <- JobSchedule.selectEnabled
    } yield assertTrue(
      rows.size == 1,
      !rows.head.enabled,
      !en.exists(s => s.kind == JobKind.History && s.clubId.contains(clubIdA))
    )
  }

  private def testSeedPerClubPreservesExisting =
    test("seedPerClubIfAbsent leaves an existing edited per-club row untouched") {
      val existing = JobSchedule(0L, JobKind.History, Some(clubIdA), None, 99, enabled = false, None)
      for {
        _    <- deleteAll
        _    <- JobSchedule.insert(existing)
        n    <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = true))
        rows <- perClubRows(JobKind.History, clubIdA)
      } yield assertTrue(
        n == 0,
        rows.size == 1,
        rows.head.intervalHours == 99,
        !rows.head.enabled
      )
    }

  private def testDeleteByClubRemovesAllKinds =
    test("deleteByClub removes all per-club rows for the club (any kind)") {
      for {
        _   <- deleteAll
        _   <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = true))
        _   <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.Membership, 24, enabled = true))
        n   <- JobSchedule.deleteByClub(clubIdA)
        all <- JobSchedule.selectAll
      } yield assertTrue(n == 2, !all.exists(_.clubId.contains(clubIdA)))
    }

  private def testDeleteByClubLeavesOtherClubs =
    test("deleteByClub leaves other clubs' rows untouched") {
      for {
        _  <- deleteAll
        _  <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = true))
        _  <- JobSchedule.seedPerClubIfAbsent(clubIdB, ScheduleSeed(JobKind.History, 24, enabled = true))
        n  <- JobSchedule.deleteByClub(clubIdA)
        ra <- perClubRows(JobKind.History, clubIdA)
        rb <- perClubRows(JobKind.History, clubIdB)
      } yield assertTrue(n == 1, ra.isEmpty, rb.size == 1)
    }

  private def testDeleteByClubLeavesGlobalRows =
    test("deleteByClub leaves global (club_id NULL) rows untouched") {
      for {
        _    <- deleteAll
        _    <- JobSchedule.seedPerClubIfAbsent(clubIdA, ScheduleSeed(JobKind.History, 24, enabled = true))
        _    <- JobSchedule.seedGlobalIfAbsent(ScheduleSeed(JobKind.MatchRef, 24, enabled = true))
        n    <- JobSchedule.deleteByClub(clubIdA)
        ra   <- perClubRows(JobKind.History, clubIdA)
        glob <- globalRows(JobKind.MatchRef)
      } yield assertTrue(n == 1, ra.isEmpty, glob.size == 1)
    }

  private def testEnsureTablesSeedsManagedOnly =
    test("ensureTables seeds History+Membership for managed clubs only") {
      for {
        _  <- deleteAll
        _  <- ManagedClub.markManaged(clubIdA, Times.t0)
        _  <- ServerTables.ensureTables
        ha <- perClubRows(JobKind.History, clubIdA)
        ma <- perClubRows(JobKind.Membership, clubIdA)
        hb <- perClubRows(JobKind.History, clubIdB)
        mb <- perClubRows(JobKind.Membership, clubIdB)
      } yield assertTrue(ha.size == 1, ma.size == 1, hb.isEmpty, mb.isEmpty)
    }
}
