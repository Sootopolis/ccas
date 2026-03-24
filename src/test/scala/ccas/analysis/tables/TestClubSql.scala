package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.Chunk

import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubUrlName, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer

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
    testClubMatchRefUpsert,
    testClubMatchRefUpsertUpdate,
    testClubMatchRefDelete,
    testClubMatchRefDeleteAll,
    testReplaceSince
  ).provideShared(
    FreshSchemaLayer("test_club_sql", onInit = Tables.ensureTables)
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
    assertCompletes
  }

  private def testDeleteAll = test("testDeleteAll") {
    for {
      _ <- ClubMatchRef.deleteAll
      _ <- ClubMember.deleteAll
      _ <- PlayerSnapshot.deleteAll
      _ <- Player.deleteAll
    } yield assertCompletes
  }

  private def testClubUpsert = test("testClubUpsert") {
    for {
      _      <- Club.upsert(clubA)
      result <- Club.selectId(clubA.clubId)
    } yield assertTrue(result.contains(clubA))
  }

  private def testClubUpsertUpdate = test("testClubUpsertUpdate") {
    val updated = clubA.copy(urlName = ClubUrlName("club-a-renamed"))
    for {
      _      <- Club.upsert(updated)
      result <- Club.selectId(clubA.clubId)
    } yield assertTrue(
      result.contains(updated),
      result.get.created == clubA.created
    )
  }

  private def testClubUpsertBatch = test("testClubUpsertBatch") {
    for {
      _        <- Club.upsertBatch(List(clubA, clubB))
      a        <- Club.selectId(clubA.clubId)
      b        <- Club.selectId(clubB.clubId)
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
      _   <- Player.insertBatch(Chunk(player0, player1, player2))
      _   <- PlayerSnapshot.insertBatch(Chunk(snap0, snap1, snap2))
      _   <- ClubMember.insert(memA0)
      all <- ClubMember.selectAll
    } yield assertTrue(all == List(memA0))
  }

  private def testMemberInsertBatch = test("testMemberInsertBatch") {
    for {
      _   <- ClubMember.insertBatch(List(memA1, memA2, memB0))
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
      clubB_all == List(memB0)
    )
  }

  private def testMemberUpdate = test("testMemberUpdate") {
    val memA0Former = memA0.copy(until = Some(Timestamps.t3))
    val memB0Former = memB0.copy(until = Some(Timestamps.t3))
    for {
      _         <- ClubMember.update(memA0Former)
      _         <- ClubMember.updateBatch(List(memB0Former))
      clubA_cur <- ClubMember.selectClubCurrent(clubA.clubId)
      clubB_cur <- ClubMember.selectClubCurrent(clubB.clubId)
    } yield assertTrue(
      clubA_cur == List(memA1),
      clubB_cur.isEmpty
    )
  }

  // --- ClubMatchRef tests ---

  private val refA = ClubMatchRef(clubA.clubId, ClubMatchId(9001), isTeam1 = true)
  private val refB = ClubMatchRef(clubB.clubId, ClubMatchId(9002), isTeam1 = false)

  private def testClubMatchRefUpsert = test("testClubMatchRefUpsert") {
    for {
      _      <- ClubMatchRef.upsert(refA)
      result <- ClubMatchRef.selectId(refA.clubId)
    } yield assertTrue(result.contains(refA))
  }

  private def testClubMatchRefUpsertUpdate = test("testClubMatchRefUpsertUpdate") {
    val updated = refA.copy(matchId = ClubMatchId(9099), isTeam1 = false)
    for {
      _      <- ClubMatchRef.upsert(updated)
      result <- ClubMatchRef.selectId(refA.clubId)
    } yield assertTrue(result.contains(updated))
  }

  private def testClubMatchRefDelete = test("testClubMatchRefDelete") {
    for {
      _      <- ClubMatchRef.upsert(refB)
      _      <- ClubMatchRef.deleteId(refB.clubId)
      result <- ClubMatchRef.selectId(refB.clubId)
    } yield assertTrue(result.isEmpty)
  }

  private def testClubMatchRefDeleteAll = test("testClubMatchRefDeleteAll") {
    for {
      _       <- ClubMatchRef.upsert(refB)
      _       <- ClubMatchRef.deleteAll
      resultA <- ClubMatchRef.selectId(refA.clubId)
      resultB <- ClubMatchRef.selectId(refB.clubId)
    } yield assertTrue(resultA.isEmpty, resultB.isEmpty)
  }

  // --- ClubMember.replaceSince tests ---

  private def testReplaceSince = test("ClubMember.replaceSince replaces approximate since with authoritative") {
    val approxMember = ClubMember(clubA.clubId, player0.playerId, Timestamps.t0, None, sinceApproximate = true)
    val newSince     = Timestamps.t1
    for {
      _       <- ClubMember.deleteAll
      _       <- ClubMember.insert(approxMember)
      updated <- ClubMember.replaceSince(clubA.clubId, player0.playerId, Timestamps.t0, newSince)
      result  <- ClubMember.selectClub(clubA.clubId)
      // Verify it does not replace a non-approximate member
      _ <- ClubMember.deleteAll
      _ <- ClubMember.insert(ClubMember(clubA.clubId, player0.playerId, Timestamps.t0, None, sinceApproximate = false))
      notUpdated <- ClubMember.replaceSince(clubA.clubId, player0.playerId, Timestamps.t0, newSince)
    } yield assertTrue(
      updated == 1,
      result.size == 1,
      result.head.since == newSince,
      !result.head.sinceApproximate,
      notUpdated == 0
    )
  }
}
