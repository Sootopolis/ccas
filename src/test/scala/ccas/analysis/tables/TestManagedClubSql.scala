package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

object TestManagedClubSql extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestManagedClubSql")(
    testMarkInsertsAndIsIdempotent,
    testSelectClubIds,
    testSelectAllWithClubOrdersAndJoins,
    testSelectAllWithClubExcludesTombstoned,
    testDelete
  ).provideShared(
    FreshSchemaLayer("test_managed_club", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
  }

  private val clubIdA = ClubId(400)
  private val clubIdB = ClubId(401)
  private val staleId = ClubId(402)

  private val reset = for {
    _ <- connectZIO { val _ = sql"DELETE FROM managed_club".update.run() }
    _ <- Club.upsert(Club(clubIdA, Times.t0, ClubSlug("club-a"), "Club A", None, None, None))
    _ <- Club.upsert(Club(clubIdB, Times.t0, ClubSlug("club-b"), "Club B", None, None, None))
    _ <- Club.upsert(Club(staleId, Times.t0, ClubSlug(s"_stale_${ClubId.unwrap(staleId)}"), "Stale", None, None, None))
  } yield ()

  private def testMarkInsertsAndIsIdempotent = test("markManaged inserts once, re-mark is a no-op") {
    for {
      _      <- reset
      first  <- ManagedClub.markManaged(clubIdA, Times.t0)
      second <- ManagedClub.markManaged(clubIdA, Times.t1)
      row    <- ManagedClub.selectByClubId(clubIdA)
    } yield assertTrue(
      first == 1,
      second == 0,
      row.exists(_.clubId == clubIdA),
      row.exists(_.markedAt == Times.t0) // first mark wins; re-mark does not overwrite markedAt
    )
  }

  private def testSelectClubIds = test("selectClubIds returns managed club ids, excluding tombstoned") {
    for {
      _   <- reset
      _   <- ManagedClub.markManaged(clubIdA, Times.t0)
      _   <- ManagedClub.markManaged(staleId, Times.t0)
      ids <- ManagedClub.selectClubIds
    } yield assertTrue(ids.toSet == Set(clubIdA)) // staleId excluded: tombstoned clubs are not valid job targets
  }

  private def testSelectAllWithClubOrdersAndJoins = test("selectAllWithClub joins club and orders newest-first") {
    for {
      _     <- reset
      _     <- ManagedClub.markManaged(clubIdA, Times.t0)
      _     <- ManagedClub.markManaged(clubIdB, Times.t1)
      views <- ManagedClub.selectAllWithClub
    } yield assertTrue(
      views.map(_.slug) == List(ClubSlug("club-b"), ClubSlug("club-a")), // t1 before t0
      views.head.name == "Club B"
    )
  }

  private def testSelectAllWithClubExcludesTombstoned = test("selectAllWithClub excludes tombstoned clubs") {
    for {
      _     <- reset
      _     <- ManagedClub.markManaged(clubIdA, Times.t0)
      _     <- ManagedClub.markManaged(staleId, Times.t1)
      views <- ManagedClub.selectAllWithClub
    } yield assertTrue(views.map(_.slug) == List(ClubSlug("club-a")))
  }

  private def testDelete = test("delete clears the marker") {
    for {
      _     <- reset
      _     <- ManagedClub.markManaged(clubIdA, Times.t0)
      rows  <- ManagedClub.delete(clubIdA)
      after <- ManagedClub.selectByClubId(clubIdA)
    } yield assertTrue(rows == 1, after.isEmpty)
  }
}
