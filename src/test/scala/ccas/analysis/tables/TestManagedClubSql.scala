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
    testSelectAllWithClubExcludesNameless,
    testDelete
  ).provideShared(
    FreshSchemaLayer("test_managed_club", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
  }

  private val clubIdA    = ClubId(400)
  private val clubIdB    = ClubId(401)
  private val namelessId = ClubId(402)
  private val takerId    = ClubId(403)

  // 402 ends holding no name because 403 takes the one it held — what the `_stale_<id>` tombstone used to stand in
  // for (#254 step 4).
  private val reset = for {
    _ <- connectZIO { sql"DELETE FROM managed_club".update.run() }
    _ <- Club.upsert(Club(clubIdA, Times.t0, ClubSlug("club-a"), "Club A", None, None, None))
    _ <- Club.upsert(Club(clubIdB, Times.t0, ClubSlug("club-b"), "Club B", None, None, None))
    _ <- Club.upsert(Club(namelessId, Times.t0, ClubSlug("club-c"), "Nameless", None, None, None))
    _ <- Club.upsert(Club(takerId, Times.t0, ClubSlug("club-c"), "Taker", None, None, None))
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

  private def testSelectClubIds = test("selectClubIds returns managed club ids, excluding one that holds no name") {
    for {
      _   <- reset
      _   <- ManagedClub.markManaged(clubIdA, Times.t0)
      _   <- ManagedClub.markManaged(namelessId, Times.t0)
      ids <- ManagedClub.selectClubIds
    } yield assertTrue(ids.toSet == Set(clubIdA)) // a club with no name to fetch under is not a valid job target
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

  private def testSelectAllWithClubExcludesNameless = test("selectAllWithClub excludes a club that holds no name") {
    for {
      _     <- reset
      _     <- ManagedClub.markManaged(clubIdA, Times.t0)
      _     <- ManagedClub.markManaged(namelessId, Times.t1)
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
