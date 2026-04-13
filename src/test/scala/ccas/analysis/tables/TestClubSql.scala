package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.Chunk

import com.augustnagro.magnum.sql

import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

object TestClubSql extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestClubSql")(
    testClubUpsert,
    testClubUpsertUpdate,
    testClubUpsertBatch,
    testClubSelect,
    testClubMembersCount,
    testMemberInsert,
    testMemberInsertBatch,
    testMemberSelect,
    testMemberUpdate,
    testClubMatchRefInsert,
    testClubMatchRefUpsert,
    testClubMatchRefDelete,
    testClubMatchRefDeleteAll,
    testReplaceSinceApproximate,
    testReplaceSinceNonApproximate,
    testClubAdminInsertAndSelect,
    testClubAdminDeleteByClub,
    testClubAdminSelectPlayerIdsForSizableClubs,
    testClubAdminReplaceForClub
  ).provideShared(
    FreshSchemaLayer("test_club_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
    val t3: Instant = t0.plus(Duration.ofDays(3))
  }

  private val clubA = Club(ClubId(200), Times.t0, ClubSlug("club-a"), "Club A", None, None, None)
  private val clubB = Club(ClubId(201), Times.t0, ClubSlug("club-b"), "Club B", None, None, None)

  private val player0 = Player(PlayerId(10), Times.t0, Username("p0"), Active, None, Times.t1)
  private val player1 = Player(PlayerId(11), Times.t0, Username("p1"), Active, None, Times.t1)
  private val player2 = Player(PlayerId(12), Times.t0, Username("p2"), Closed, None, Times.t1)

  // Club A: player0 (current), player1 (current), player2 (former)
  // Club B: player0 (current)
  private val memA0 = ClubMember(clubA.clubId, player0.playerId, Times.t1, None)
  private val memA1 = ClubMember(clubA.clubId, player1.playerId, Times.t1, None)
  private val memA2 = ClubMember(clubA.clubId, player2.playerId, Times.t1, Some(Times.t2))
  private val memB0 = ClubMember(clubB.clubId, player0.playerId, Times.t1, None)

  // --- Club tests ---

  private def testClubUpsert = test("testClubUpsert") {
    for {
      _      <- Club.upsert(clubA)
      result <- Club.selectId(clubA.clubId)
    } yield assertTrue(result.contains(clubA))
  }

  private def testClubUpsertUpdate = test("testClubUpsertUpdate") {
    val updated = clubA.copy(slug = ClubSlug("club-a-renamed"))
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
    val memA0Former = memA0.copy(until = Some(Times.t3))
    val memB0Former = memB0.copy(until = Some(Times.t3))
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

  private val refA = ClubMatchRef(clubA.clubId, ClubMatchId(9001), isLive = false, isTeam1 = true)
  private val refB = ClubMatchRef(clubB.clubId, ClubMatchId(9002), isLive = false, isTeam1 = false)

  private def testClubMatchRefInsert = test("testClubMatchRefInsert") {
    for {
      _      <- ClubMatchRef.insert(refA)
      result <- ClubMatchRef.selectId(refA.clubId)
    } yield assertTrue(result.contains(refA))
  }

  // upsert refreshes all non-PK columns. This protects against races between RefApp resolving
  // a club and RecruitmentApp writing a match ref for the same club — both fibers can write without
  // hitting a duplicate-key crash.
  private def testClubMatchRefUpsert = test("testClubMatchRefUpsert") {
    val refreshed = refA.copy(matchId = ClubMatchId(9999), isLive = true, isTeam1 = false)
    for {
      _      <- ClubMatchRef.upsert(refreshed)
      result <- ClubMatchRef.selectId(refA.clubId)
    } yield assertTrue(result.contains(refreshed))
  }

  private def testClubMatchRefDelete = test("testClubMatchRefDelete") {
    for {
      _      <- ClubMatchRef.insert(refB)
      _      <- ClubMatchRef.deleteId(refB.clubId)
      result <- ClubMatchRef.selectId(refB.clubId)
    } yield assertTrue(result.isEmpty)
  }

  private def testClubMatchRefDeleteAll = test("testClubMatchRefDeleteAll") {
    for {
      _       <- ClubMatchRef.insert(refB)
      _       <- connectZIO(sql"DELETE FROM club_match_ref".update.run())
      resultA <- ClubMatchRef.selectId(refA.clubId)
      resultB <- ClubMatchRef.selectId(refB.clubId)
    } yield assertTrue(resultA.isEmpty, resultB.isEmpty)
  }

  // --- ClubMember.replaceSince tests ---

  private def testReplaceSinceApproximate =
    test("ClubMember.replaceSince replaces approximate since with authoritative") {
      val approxMember = ClubMember(clubA.clubId, player0.playerId, Times.t0, None, sinceApproximate = true)
      val newSince     = Times.t1
      for {
        _       <- connectZIO(sql"DELETE FROM club_member".update.run())
        _       <- ClubMember.insert(approxMember)
        updated <- ClubMember.replaceSince(clubA.clubId, player0.playerId, Times.t0, newSince)
        result  <- ClubMember.selectClub(clubA.clubId)
      } yield assertTrue(
        updated == 1,
        result.size == 1,
        result.head.since == newSince,
        !result.head.sinceApproximate
      )
    }

  private def testReplaceSinceNonApproximate =
    test("ClubMember.replaceSince does not replace non-approximate") {
      for {
        _ <- connectZIO(sql"DELETE FROM club_member".update.run())
        _ <- ClubMember.insert(ClubMember(clubA.clubId, player0.playerId, Times.t0, None, sinceApproximate = false))
        notUpdated <- ClubMember.replaceSince(clubA.clubId, player0.playerId, Times.t0, Times.t1)
      } yield assertTrue(notUpdated == 0)
    }

  // --- Club.membersCount / latestMatchAt tests ---

  private def testClubMembersCount = test("upsert stores membersCount; updateLatestMatchAt stores latestMatchAt") {
    val withCount = clubA.copy(membersCount = Some(1234))
    for {
      _              <- Club.upsert(withCount)
      _              <- Club.updateLatestMatchAt(clubA.clubId, Some(Times.t1))
      result         <- Club.selectId(clubA.clubId)
      bySlug         <- Club.selectBySlug(clubA.slug)
      // upsert must NOT clobber latest_match_at on subsequent calls
      _              <- Club.upsert(withCount.copy(name = "Renamed"))
      preserved      <- Club.selectId(clubA.clubId)
    } yield assertTrue(
      result.get.membersCount.contains(1234),
      result.get.latestMatchAt.contains(Times.t1),
      bySlug.get.membersCount.contains(1234),
      bySlug.get.latestMatchAt.contains(Times.t1),
      preserved.get.name == "Renamed",
      preserved.get.latestMatchAt.contains(Times.t1)
    )
  }

  // --- ClubAdmin tests ---

  private def testClubAdminInsertAndSelect = test("ClubAdmin insert and select") {
    val admin0 = ClubAdmin(clubA.clubId, player0.playerId)
    val admin1 = ClubAdmin(clubA.clubId, player1.playerId)
    for {
      _       <- ClubAdmin.insertBatch(List(admin0, admin1))
      byClub  <- ClubAdmin.selectByClub(clubA.clubId)
      ids     <- ClubAdmin.selectPlayerIdsByClub(clubA.clubId)
      emptyB  <- ClubAdmin.selectByClub(clubB.clubId)
    } yield assertTrue(
      byClub.toSet == Set(admin0, admin1),
      ids == Set(player0.playerId, player1.playerId),
      emptyB.isEmpty
    )
  }

  private def testClubAdminDeleteByClub = test("ClubAdmin deleteByClub removes all admins for club") {
    for {
      deleted <- ClubAdmin.deleteByClub(clubA.clubId)
      after   <- ClubAdmin.selectByClub(clubA.clubId)
    } yield assertTrue(deleted == 2, after.isEmpty)
  }

  private def testClubAdminSelectPlayerIdsForSizableClubs = test("selectPlayerIdsForSizableClubs filters by size, inactivity, and admin ratio") {
    val adminA = ClubAdmin(clubA.clubId, player0.playerId)
    val adminB = ClubAdmin(clubB.clubId, player1.playerId)
    val now    = Instant.now()
    val stale  = now.minus(java.time.Duration.ofDays(400)) // > 1 year
    for {
      // --- Baseline: active sizable club includes its admin ---
      _           <- Club.upsert(clubA.copy(membersCount = Some(1234)))
      _           <- Club.updateLatestMatchAt(clubA.clubId, Some(now))
      _           <- ClubAdmin.insertBatch(List(adminA, adminB))
      sizable     <- ClubAdmin.selectPlayerIdsForSizableClubs(1000)
      noneSizable <- ClubAdmin.selectPlayerIdsForSizableClubs(2000)

      // --- Inactive club is ignored ---
      _        <- Club.updateLatestMatchAt(clubA.clubId, Some(stale))
      inactive <- ClubAdmin.selectPlayerIdsForSizableClubs(1000)

      // --- Over-administered club is ignored: 1 admin out of 4 members = 25% ratio ---
      _      <- Club.upsert(clubA.copy(membersCount = Some(4)))
      _      <- Club.updateLatestMatchAt(clubA.clubId, Some(now))
      gimmic <- ClubAdmin.selectPlayerIdsForSizableClubs(1)

      // Cleanup
      _ <- ClubAdmin.deleteByClub(clubA.clubId)
      _ <- ClubAdmin.deleteByClub(clubB.clubId)
    } yield assertTrue(
      sizable == Set(player0.playerId),
      noneSizable.isEmpty,
      inactive.isEmpty,
      gimmic.isEmpty
    )
  }

  private def testClubAdminReplaceForClub = test("replaceForClub atomically replaces admin set") {
    for {
      // Seed initial admins
      _      <- ClubAdmin.insertBatch(List(ClubAdmin(clubA.clubId, player0.playerId)))
      before <- ClubAdmin.selectPlayerIdsByClub(clubA.clubId)
      // Replace with a different set
      _      <- ClubAdmin.replaceForClub(clubA.clubId, Set(player1.playerId, player2.playerId))
      after  <- ClubAdmin.selectPlayerIdsByClub(clubA.clubId)
      // Replace with empty set should clear all rows
      _      <- ClubAdmin.replaceForClub(clubA.clubId, Set.empty)
      empty  <- ClubAdmin.selectPlayerIdsByClub(clubA.clubId)
    } yield assertTrue(
      before == Set(player0.playerId),
      after == Set(player1.playerId, player2.playerId),
      empty.isEmpty
    )
  }
}
