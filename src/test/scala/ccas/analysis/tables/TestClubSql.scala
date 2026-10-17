package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.{Chunk, Ref, ZIO}
import zio.http.*

import com.augustnagro.magnum.sql

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiDailyMatchJson
import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.{ForcedOverlap, FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

object TestClubSql extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestClubSql")(
    testClubUpsert,
    testClubUpsertUpdate,
    testClubSelectId,
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
    testSlugFromMatchRefRejectsABoardNamingNoClub,
    testTwoClubsMayHoldOneSlug,
    testClubAdminInsertAndSelect,
    testClubAdminDeleteByClub,
    testClubAdminSelectPlayerIdsForSizableClubs,
    testClubAdminReplaceForClub,
    testAnEndedRowSaysWhenLastKnownIn,
    testOverlappingRowsAreRefused,
    testACountAtAMomentReadsTheRowsAlone,
    testCoverageOnlyWidens,
    testEveryCoverageWriteChangesItsVersion,
    testASaveQueuesBehindAnotherAndReadsItsVersion,
    testAKeptListSpeaksForAClubUnlessOlderThanItsPlayer
  ).provideShared(
    FreshSchemaLayer("test_club_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

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
  private val memA0 = ClubMember.joined(clubId = clubA.clubId, playerId = player0.playerId, since = Times.t1)
  private val memA1 = ClubMember.joined(clubId = clubA.clubId, playerId = player1.playerId, since = Times.t1)
  private val memA2 = ClubMember(
    clubId = clubA.clubId,
    playerId = player2.playerId,
    since = Times.t1,
    lastSeen = Some(Times.t1),
    until = Some(Times.t2)
  )
  private val memB0 = ClubMember.joined(clubId = clubB.clubId, playerId = player0.playerId, since = Times.t1)

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

  private def testClubSelectId = test("selectId finds each upserted club and misses an unknown id") {
    for {
      _        <- ZIO.foreachDiscard(List(clubA, clubB))(Club.upsert)
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
      _   <- Player.writeBatch(inserted = Chunk(player0, player1, player2), updated = Nil)
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
      clubA_cur <- ClubMember.selectClubListed(clubA.clubId)
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
    val memA0Former = memA0.copy(lastSeen = Some(Times.t2), until = Some(Times.t3))
    val memB0Former = memB0.copy(lastSeen = Some(Times.t2), until = Some(Times.t3))
    for {
      _         <- ClubMember.update(memA0Former)
      _         <- ClubMember.updateBatch(List(memB0Former))
      clubA_cur <- ClubMember.selectClubListed(clubA.clubId)
      clubB_cur <- ClubMember.selectClubListed(clubB.clubId)
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

  // A board that names no club is a malformed response, not "no rename to be had": Tier B must see the difference,
  // since its answer to absence is to move on quietly (ADR 0019).
  private def testSlugFromMatchRefRejectsABoardNamingNoClub =
    test("slugFromMatchRef fails when the board's team URL names no club") {
      val club    = Club(ClubId(240), Times.t0, ClubSlug("board-says-nothing"), "Boardless", None, None, None)
      val matchId = ClubMatchId(9610)
      val matchJson = apiDailyMatchJson(
        matchId = ClubMatchId.unwrap(matchId),
        team1Club = "renamed-club",
        team2Club = "opponent-club",
        team1Players = List(("p1", 1)),
        team2Players = List(("p2", 1))
      ).replace("https://api.chess.com/pub/club/renamed-club", "https://api.chess.com")
      val routes = Routes(
        Method.GET / "pub" / "match" / long("matchId") -> handler((_: Long, _: Request) => Response.json(matchJson))
      )
      for {
        _      <- Club.upsert(club)
        _      <- ClubMatchRef.insert(ClubMatchRef(club.clubId, matchId, isLive = false, isTeam1 = true))
        client <- TestChessComClientSupport.fakeClient(routes)
        result <- Club.slugFromMatchRef(club.clubId, client).either
      } yield assertTrue(result.left.exists(_.isInstanceOf[IllegalArgumentException]))
    }

  // `club.slug` is a display cache with no unique index since #254, and only `club_name` says who holds a name now.
  // The second upsert here is what a `club_slug_key` still in place would reject.
  private def testTwoClubsMayHoldOneSlug =
    test("two clubs may hold one slug in `club`, and `club_name` still names a single current holder") {
      val slug    = ClubSlug("shared-slug")
      val former  = Club(ClubId(230), Times.t0, slug, "Former Holder", None, None, None)
      val current = Club(ClubId(231), Times.t0, slug, "Current Holder", None, None, None)
      for {
        _            <- Club.upsert(former)
        _            <- Club.upsert(current)
        sharing      <- Club.selectAll.map(_.filter(_.slug == slug).map(_.clubId).toSet)
        holderOption <- ClubName.selectCurrentHolder(slug)
      } yield assertTrue(
        sharing == Set(former.clubId, current.clubId),
        holderOption.map(_.clubId).contains(current.clubId)
      )
    }

  // --- Club.membersCount / latestMatchAt tests ---

  private def testClubMembersCount = test("upsert stores membersCount; updateLatestMatchAt stores latestMatchAt") {
    val withCount = clubA.copy(membersCount = Some(1234))
    for {
      _              <- Club.upsert(withCount)
      _              <- Club.updateLatestMatchAt(clubA.clubId, Some(Times.t1))
      result         <- Club.selectId(clubA.clubId)
      // upsert must NOT clobber latest_match_at on subsequent calls
      _              <- Club.upsert(withCount.copy(name = "Renamed"))
      preserved      <- Club.selectId(clubA.clubId)
    } yield assertTrue(
      result.get.membersCount.contains(1234),
      result.get.latestMatchAt.contains(Times.t1),
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
    val stale  = now.minus(Duration.ofDays(400)) // > 1 year
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

  // --- Member row windows (ADR 0021) ---

  private val joinedAtT0 = ClubMember.joined(clubId = clubA.clubId, playerId = player1.playerId, since = Times.t0)

  private def refusedBy(constraint: String)(insert: ZIO[PostgresClient, Throwable, Any]) =
    insert.exit.map(_.causeOption.flatMap(_.failureOption).exists(_.getMessage.contains(constraint)))

  /** A row with `last_seen` and no `until` is a membership a closed account hides (ADR 0022). */
  private def testAnEndedRowSaysWhenLastKnownIn =
    test("an ended row must say when the player was last known in, and inside the row; a hidden row only the first") {
      val window = refusedBy("club_member_window")
      for {
        _          <- connectZIO(sql"DELETE FROM club_member".update.run())
        noLast     <- window(ClubMember.insert(joinedAtT0.copy(until = Some(Times.t2))))
        afterEnd   <- window(ClubMember.insert(joinedAtT0.copy(lastSeen = Some(Times.t3), until = Some(Times.t2))))
        beforeOpen <- window(ClubMember.insert(joinedAtT0.copy(since = Times.t2).hidden(Times.t1)))
        hidden     <- ClubMember.insert(joinedAtT0.hidden(Times.t1))
        _          <- connectZIO(sql"DELETE FROM club_member".update.run())
        ended      <- ClubMember.insert(joinedAtT0.copy(lastSeen = Some(Times.t1), until = Some(Times.t2)))
      } yield assertTrue(noLast, afterEnd, beforeOpen, hidden == 1, ended == 1)
    }

  private def testOverlappingRowsAreRefused =
    test("two rows of one player in one club may meet but not overlap") {
      val ended = joinedAtT0.copy(lastSeen = Some(Times.t1), until = Some(Times.t2))
      for {
        _        <- connectZIO(sql"DELETE FROM club_member".update.run())
        _        <- ClubMember.insert(ended)
        overlap  <- refusedBy("club_member_no_overlap")(ClubMember.insert(joinedAtT0.copy(since = Times.t1)))
        adjacent <- ClubMember.insert(joinedAtT0.copy(since = Times.t2))
      } yield assertTrue(overlap, adjacent == 1)
    }

  /** A closed account's row counts until it was last seen, when the report dates its closure (ADR 0022). */
  private def testACountAtAMomentReadsTheRowsAlone =
    test("a count at a moment takes an ended row until it ended, a hidden one until last seen, whatever the status") {
      def joined(player: Player, since: Instant) =
        ClubMember.joined(clubId = clubA.clubId, playerId = player.playerId, since = since)
      val ended  = joinedAtT0.copy(lastSeen = Some(Times.t1), until = Some(Times.t2))
      val hidden = joined(player2, Times.t0).hidden(Times.t1)
      val open   = joined(player0, Times.t1)
      for {
        _      <- connectZIO(sql"DELETE FROM club_member".update.run())
        _      <- ClubMember.insertBatch(List(ended, hidden, open))
        counts <- ZIO.foreach(List(Times.t0, Times.t1, Times.t2, Times.t3))(ClubMember.countAt(clubA.clubId, _))
      } yield assertTrue(counts == List(2, 2, 1, 1))
    }

  private def testCoverageOnlyWidens = test("a club's roster coverage only ever widens") {
    for {
      _      <- connectZIO(sql"DELETE FROM club_roster_coverage".update.run())
      _      <- ClubRosterCoverage.observe(clubA.clubId, Times.t2)
      _      <- ClubRosterCoverage.observe(clubA.clubId, Times.t1)
      _      <- ClubRosterCoverage.observe(clubA.clubId, Times.t3)
      _      <- ClubRosterCoverage.observe(clubA.clubId, Times.t2)
      result <- ClubRosterCoverage.select(clubA.clubId)
    } yield assertTrue(result.contains(ClubRosterCoverage(clubA.clubId, Times.t1, Times.t3)))
  }

  // A save its bounds don't move is still a save another reconcile must notice (#312).
  private def testEveryCoverageWriteChangesItsVersion =
    test("every write of a club's roster coverage changes its version, even one that moves neither bound") {
      for {
        _      <- connectZIO(sql"DELETE FROM club_roster_coverage".update.run())
        before <- ClubRosterCoverage.selectVersion(clubA.clubId)
        _      <- ClubRosterCoverage.observe(clubA.clubId, Times.t2)
        first  <- ClubRosterCoverage.selectVersion(clubA.clubId)
        _      <- ClubRosterCoverage.observe(clubA.clubId, Times.t2)
        second <- ClubRosterCoverage.selectVersion(clubA.clubId)
      } yield assertTrue(before.isEmpty, first.isDefined, second.isDefined, second != first)
    }

  // #312: two reconciles of one club saving at once would both find the version they read and both save.
  private def testASaveQueuesBehindAnotherAndReadsItsVersion =
    test("a save of a club's roster waits for one in progress, then reads the version it left") {
      for {
        _    <- connectZIO(sql"DELETE FROM club_roster_coverage".update.run())
        seen <- Ref.make(Option.empty[Long])
        overlap <- ForcedOverlap.run(
          first = ClubRosterCoverage.lockVersion(clubA.clubId) *> ClubRosterCoverage.observe(clubA.clubId, Times.t1),
          second = ClubRosterCoverage.lockVersion(clubA.clubId).flatMap(seen.set)
        )
        left      <- ClubRosterCoverage.selectVersion(clubA.clubId)
        secondSaw <- seen.get
      } yield assertTrue(overlap.waitedOn == "advisory", left.isDefined, secondSaw == left)
    }

  /** A list kept before its player's latest state may predate the account reopening (ADR 0022). */
  private def testAKeptListSpeaksForAClubUnlessOlderThanItsPlayer =
    test("a kept list speaks for a club under any slug it held, unless older than its player's latest state") {
      def listAt(fetchedAt: Instant) = PlayerClubList(
        playerId = player2.playerId,
        fetchedAt = fetchedAt,
        lastOnlineOption = Some(Times.t0),
        clubs = Map(clubA.slug -> Times.t1)
      )
      for {
        _     <- Player.writeBatch(inserted = List(player2), updated = Nil)
        _     <- Club.upsert(clubA.copy(slug = ClubSlug("club-a-renamed")))
        _     <- PlayerClubList.save(listAt(Times.t0))
        stale <- PlayerClubList.selectListings(clubA.clubId)
        _     <- PlayerClubList.save(listAt(Times.t2))
        fresh <- PlayerClubList.selectListings(clubA.clubId)
      } yield assertTrue(stale.isEmpty, fresh == List(PlayerClubList.Listing(player2.playerId, Times.t0, Times.t1)))
    }
}
