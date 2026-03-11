package ccas.analysis.tables

import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.{ClubId, ClubUrlName, PlayerId, Username}
import ccas.utils.sql.DataSourceLayer
import zio.Chunk
import zio.test.{Spec, TestAspect, ZIOSpecDefault, assertCompletes, assertTrue}

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

object TestClubSql extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestClubSql")(
    testCreateTables,
    testDeleteAll,
    testClubUpsert,
    testClubUpsertUpdate,
    testClubUpsertBatch,
    testClubSelect,
    testMemberInsert,
    testMemberInsertBatch,
    testMemberSelect,
    testMemberUpdate,
  ).provideShared(
    DataSourceLayer.liveFromPrefix()
  ) @@ TestAspect.sequential

  private object Timestamps {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
    val t3: Instant = t0.plus(Duration.ofDays(3))
  }

  private val clubA = Club(ClubId(200), Timestamps.t0, ClubUrlName("club-a"))
  private val clubB = Club(ClubId(201), Timestamps.t0, ClubUrlName("club-b"))

  private val player0 = Player(PlayerId(10), Timestamps.t0)
  private val player1 = Player(PlayerId(11), Timestamps.t0)
  private val player2 = Player(PlayerId(12), Timestamps.t0)

  // Latest snapshots: player0 Active, player1 Active, player2 Closed
  private val snap0 = PlayerSnapshot(player0.playerId, Timestamps.t1, Username("p0"), Active, None)
  private val snap1 = PlayerSnapshot(player1.playerId, Timestamps.t1, Username("p1"), Active, None)
  private val snap2 = PlayerSnapshot(player2.playerId, Timestamps.t1, Username("p2"), Closed, None)

  // Club A: player0 (current), player1 (current), player2 (former)
  // Club B: player0 (current)
  private val memA0 = ClubMember(clubA.clubId, player0.playerId, Timestamps.t1, None)
  private val memA1 = ClubMember(clubA.clubId, player1.playerId, Timestamps.t1, None)
  private val memA2 = ClubMember(clubA.clubId, player2.playerId, Timestamps.t1, Some(Timestamps.t2))
  private val memB0 = ClubMember(clubB.clubId, player0.playerId, Timestamps.t1, None)

  // --- Club tests ---

  private def testCreateTables = test("testCreateTables") {
    for {
      _ <- Player.createTable
      _ <- PlayerSnapshot.createTable
      _ <- Club.createTable
      _ <- ClubMember.createTable
    } yield assertCompletes
  }

  private def testDeleteAll = test("testDeleteAll") {
    for {
      _ <- ClubMember.deleteAll
      _ <- PlayerSnapshot.deleteAll
      _ <- Player.deleteAll
    } yield assertCompletes
  }

  private def testClubUpsert = test("testClubUpsert") {
    for {
      _ <- Club.upsert(clubA)
      result <- Club.selectId(clubA.clubId)
    } yield assertTrue(result.contains(clubA))
  }

  private def testClubUpsertUpdate = test("testClubUpsertUpdate") {
    val updated = clubA.copy(urlName = ClubUrlName("club-a-renamed"))
    for {
      _ <- Club.upsert(updated)
      result <- Club.selectId(clubA.clubId)
    } yield assertTrue(
      result.contains(updated),
      result.get.created == clubA.created
    )
  }

  private def testClubUpsertBatch = test("testClubUpsertBatch") {
    for {
      _ <- Club.upsertBatch(List(clubA, clubB))
      a <- Club.selectId(clubA.clubId)
      b <- Club.selectId(clubB.clubId)
      notFound <- Club.selectId(ClubId(999))
    } yield assertTrue(
      a.contains(clubA),
      b.contains(clubB),
      notFound.isEmpty
    )
  }

  private def testClubSelect = test("testClubSelect") {
    for {
      all <- Club.selectAll
    } yield assertTrue(Set(clubA, clubB).subsetOf(all.toSet))
  }

  // --- ClubMember tests ---

  private def testMemberInsert = test("testMemberInsert") {
    for {
      _ <- Player.insertBatch(Chunk(player0, player1, player2))
      _ <- PlayerSnapshot.insertBatch(Chunk(snap0, snap1, snap2))
      _ <- ClubMember.insert(memA0)
      all <- ClubMember.selectAll
    } yield assertTrue(all == List(memA0))
  }

  private def testMemberInsertBatch = test("testMemberInsertBatch") {
    for {
      _ <- ClubMember.insertBatch(List(memA1, memA2, memB0))
      all <- ClubMember.selectAll
    } yield assertTrue(all.toSet == Set(memA0, memA1, memA2, memB0))
  }

  private def testMemberSelect = test("testMemberSelect") {
    for {
      all       <- ClubMember.selectAll
      clubA_all <- ClubMember.selectClub(clubA.clubId)
      clubA_cur <- ClubMember.selectClubCurrent(clubA.clubId)
      clubA_act <- ClubMember.selectClubActive(clubA.clubId)
      clubA_fmr <- ClubMember.selectClubFormer(clubA.clubId)
      clubB_all <- ClubMember.selectClub(clubB.clubId)
    } yield assertTrue(
      all.toSet == Set(memA0, memA1, memA2, memB0),
      clubA_all.toSet == Set(memA0, memA1, memA2),
      clubA_cur.toSet == Set(memA0, memA1),
      clubA_act.toSet == Set(memA0, memA1),
      clubA_fmr == List(memA2),
      clubB_all == List(memB0),
    )
  }

  private def testMemberUpdate = test("testMemberUpdate") {
    val memA0Former = memA0.copy(until = Some(Timestamps.t3))
    val memB0Former = memB0.copy(until = Some(Timestamps.t3))
    for {
      _ <- ClubMember.update(memA0Former)
      _ <- ClubMember.updateBatch(List(memB0Former))
      clubA_cur <- ClubMember.selectClubCurrent(clubA.clubId)
      clubB_cur <- ClubMember.selectClubCurrent(clubB.clubId)
    } yield assertTrue(
      clubA_cur == List(memA1),
      clubB_cur.isEmpty,
    )
  }
}
