package ccas.analysis.apps.membership

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.{Chunk, Promise, Ref, UIO, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import TestMembershipAppSupport.*

import ccas.analysis.apps.{NamedClub, PlayerUpdater}
import ccas.analysis.apps.ClubSlugRenameResolver.ClubMismatchException
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.apps.membership.MembershipClassify.{PhaseBResult, PhaseCResult}
import ccas.analysis.apps.membership.MembershipReport.InferredMember
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{
  apiClubJson,
  apiClubMembersJson,
  apiDailyMatchJson,
  apiMatchBoardJson,
  apiPlayerJson
}
import ccas.analysis.tables.{
  Club,
  ClubMember,
  ClubRosterCoverage,
  MembershipRun,
  Player,
  PlayerClubList,
  PlayerMatchRef,
  PlayerName,
  RunTrigger,
  Tables
}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient, TestDbCleanup}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient.{connectZIO, withTransaction}

object TestMembershipAppIntegration extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestMembershipAppIntegration")(
    suiteLookupJoinInvitations,
    suiteLoadStoredRoster,
    suiteExternalMemberDetection,
    suiteClassifyApiMembers,
    suiteClassifyDisappeared,
    suiteReconcile,
    suiteReport
  ).provideShared(
    FreshSchemaLayer("test_membership_app_integration", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite D: lookupJoinInvitations (DB)
  // ==========================================================================

  private def suiteLookupJoinInvitations = suite("lookupJoinInvitations")(
    testReturnsInvitationForOurClub,
    testDoesNotReturnInvitationFromDifferentClub,
    testSkipsLookupForNonJoinChanges
  )

  private def testReturnsInvitationForOurClub = test("returns invitation for player invited by our club") {
    val player    = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    val summaries = List(MemberChangeSummary(pid0, Username("alice"), Chunk(NewMember(Times.t2))))
    for {
      _      <- seedDb(players = List(player))
      _      <- seedRecruitmentInvitation(clubId, pid0, Times.t1)
      result <- MembershipReport.lookupJoinInvitations(clubId, summaries)
    } yield assertTrue(
      result.size == 1,
      result(pid0) == Times.t1
    )
  }

  private def testDoesNotReturnInvitationFromDifferentClub = test("does NOT return invitation from a different club") {
    val player    = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
    val summaries = List(MemberChangeSummary(pid1, Username("bob"), Chunk(NewMember(Times.t2))))
    for {
      _      <- seedDb(players = List(player))
      _      <- Club.upsert(otherClub)
      _      <- seedRecruitmentInvitation(otherClubId, pid1, Times.t1)
      result <- MembershipReport.lookupJoinInvitations(clubId, summaries)
    } yield assertTrue(result.isEmpty)
  }

  private def testSkipsLookupForNonJoinChanges = test("skips lookup for non-join changes") {
    val summaries = List(
      MemberChangeSummary(pid0, Username("alice"), Chunk(UsernameChange(Times.t1, Username("old"))))
    )
    for {
      result <- MembershipReport.lookupJoinInvitations(clubId, summaries)
    } yield assertTrue(result.isEmpty)
  }

  // ==========================================================================
  // Suite E: loadStoredRoster (DB)
  // ==========================================================================

  private def suiteLoadStoredRoster = suite("loadStoredRoster")(
    testLoadsTheStoredRosterMaps,
    testExcludesFormerMembers
  )

  private def testLoadsTheStoredRosterMaps = test("loads the stored roster maps") {
    val player0 = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
    val player1 = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t1)
    val mem0    = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t1)
    val mem1    = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t1)

    for {
      _ <- seedDb(
        players = List(player0, player1),
        members = List(mem0, mem1)
      )
      storedRoster <- MembershipApp.loadStoredRoster(clubId)
    } yield assertTrue(
      storedRoster.membersByPlayerId.size == 2,
      storedRoster.membersByPlayerId.contains(pid0),
      storedRoster.membersByPlayerId.contains(pid1),
      storedRoster.membersByPlayerId(pid0).player == player0,
      storedRoster.membersByPlayerId(pid0).member == mem0,
      storedRoster.membersByUsername.size == 2,
      storedRoster.membersByUsername.contains(Username("alice")),
      storedRoster.membersByUsername.contains(Username("bob")),
      storedRoster.knownPlayersByUsername.contains(Username("alice")),
      storedRoster.knownPlayersByUsername.contains(Username("bob")),
      storedRoster.knownPlayersByUsername(Username("alice")) == player0,
      storedRoster.knownPlayersByUsername(Username("bob")) == player1
    )
  }

  private def testExcludesFormerMembers = test("excludes former members from StoredRoster") {
    val player0 = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
    val formerMem = ClubMember(
      clubId = clubId,
      playerId = pid0,
      since = Times.t0,
      lastSeen = Some(Times.t0),
      until = Some(Times.t1)
    )

    for {
      _ <- seedDb(
        players = List(player0),
        members = List(formerMem)
      )
      storedRoster <- MembershipApp.loadStoredRoster(clubId)
    } yield assertTrue(
      storedRoster.membersByPlayerId.isEmpty,
      storedRoster.knownPlayersByUsername.contains(Username("alice"))
    )
  }

  // ==========================================================================
  // Suite F: external member detection (DB)
  // ==========================================================================

  private def suiteExternalMemberDetection = suite("external member detection")(
    testMergeResultsIncludesExternalChanges,
    testSelectPlayerIdsCurrentAtReturnsCurrentMembers,
    testSelectPlayerIdsCurrentAtExcludesLeftMembers,
    testCountActiveCurrentAtExcludesNonActive,
    testSelectLatestCompletedReturnsCompletedRuns,
    testSelectLatestCompletedReturnsNoneWhenNoCompletedRuns,
    testSelectLatestReportedSkipsIncompleteAndDependencyRuns,
    testSelectLatestReportedTakesTheLastToComplete
  )

  private def testMergeResultsIncludesExternalChanges = test("mergeResults includes external changes and memberships") {
    val phaseB = PhaseBResult(
      resolvedIds = Set(pid0),
      changes = Chunk.empty,
      newPlayers = Chunk.empty,
      updatedPlayers = Chunk.empty,
      archivedSnapshots = Chunk.empty,
      newMemberships = Chunk.empty,
      closedMemberships = Chunk.empty,
      reopenedMemberships = Chunk.empty
    )
    val phaseC    = PhaseCResult(Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty)
    val extChange = MemberChangeSummary(pid1, Username("bob"), Chunk(JoinedClub(Times.t1)))
    val extMember = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t1)
    val result = MembershipApp.mergeResults(
      clubId = clubId,
      b = phaseB,
      c = phaseC,
      settlement = noSettlement,
      currentMemberCount = 10,
      previousMemberCount = 8,
      startedAt = Times.t0,
      completedAt = Times.t1,
      externalChanges = Chunk(extChange),
      externalMemberships = Chunk(extMember)
    )
    assertTrue(
      result.changes == Chunk(extChange),
      result.newMemberships == Chunk(extMember),
      result.currentMemberCount == 10,
      result.previousMemberCount == 8
    )
  }

  private def testSelectPlayerIdsCurrentAtReturnsCurrentMembers =
    test("selectPlayerIdsCurrentAt returns members current at given time") {
      val player0 = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val player1 = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
      // alice: current member since t0
      val mem0 = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
      // bob: joined at t2, so not current at t1
      val mem1 = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t2)

      for {
        _    <- seedDb(players = List(player0, player1), members = List(mem0, mem1))
        atT1 <- ClubMember.selectPlayerIdsCurrentAt(clubId, Times.t1)
        atT3 <- ClubMember.selectPlayerIdsCurrentAt(clubId, Times.t3)
      } yield assertTrue(
        atT1 == Set(pid0),
        atT3 == Set(pid0, pid1)
      )
    }

  private def testSelectPlayerIdsCurrentAtExcludesLeftMembers =
    test("selectPlayerIdsCurrentAt excludes members who left before the given time") {
      val player0 = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      // alice: was member from t0 to t1
      val mem0 = ClubMember(
        clubId = clubId,
        playerId = pid0,
        since = Times.t0,
        lastSeen = Some(Times.t0),
        until = Some(Times.t1)
      )

      for {
        _    <- seedDb(players = List(player0), members = List(mem0))
        atT0 <- ClubMember.selectPlayerIdsCurrentAt(clubId, Times.t0)
        atT2 <- ClubMember.selectPlayerIdsCurrentAt(clubId, Times.t2)
      } yield assertTrue(
        atT0 == Set(pid0),
        atT2.isEmpty
      )
    }

  private def testCountActiveCurrentAtExcludesNonActive =
    test("countActiveCurrentAt excludes non-Active players and players outside the window") {
      val alice = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val bob   = Player(pid1, Times.t0, Username("bob"), Closed, None, Times.t0)
      val carol = Player(pid2, Times.t0, Username("carol"), Active, None, Times.t0)
      // alice: active, current → counted
      val memAlice = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
      // bob: closed but still current → excluded by status filter
      val memBob = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t0)
      // carol: active but left at t1 → excluded by window at t2
      val memCarol = ClubMember(
        clubId = clubId,
        playerId = pid2,
        since = Times.t0,
        lastSeen = Some(Times.t0),
        until = Some(Times.t1)
      )

      for {
        _      <- seedDb(players = List(alice, bob, carol), members = List(memAlice, memBob, memCarol))
        atT0p5 <- ClubMember.countActiveCurrentAt(clubId, Times.t0.plusSeconds(30))
        atT2   <- ClubMember.countActiveCurrentAt(clubId, Times.t2)
      } yield assertTrue(
        atT0p5 == 2, // alice + carol (both active, both current at t0+30s)
        atT2 == 1    // alice only (carol has left, bob is Closed)
      )
    }

  private def testSelectLatestCompletedReturnsCompletedRuns = test("selectLatestCompleted returns only completed runs") {
    for {
      _ <- seedDb()
      // Insert a completed run
      runId1 <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t0, None)
      _      <- MembershipRun.complete(runId1, Times.t1)
      // Insert an incomplete run (no completedAt)
      _      <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t2, None)
      result <- MembershipRun.selectLatestCompleted(clubId)
    } yield assertTrue(
      result.isDefined,
      result.get.startedAt == Times.t0,
      result.get.completedAt.contains(Times.t1)
    )
  }

  private def testSelectLatestCompletedReturnsNoneWhenNoCompletedRuns =
    test("selectLatestCompleted returns None when no completed runs exist") {
      for {
        _      <- seedDb()
        _      <- connectZIO(sql"DELETE FROM membership_run WHERE club_id = $clubId".update.run())
        _      <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t0, None)
        result <- MembershipRun.selectLatestCompleted(clubId)
      } yield assertTrue(result.isEmpty)
    }

  private def testSelectLatestReportedSkipsIncompleteAndDependencyRuns =
    test("selectLatestReported skips incomplete runs and runs another job made") {
      for {
        _          <- seedDb()
        _          <- deleteRuns
        reported   <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t0, None)
        _          <- MembershipRun.complete(reported, Times.t1)
        dependency <- MembershipRun.insert(clubId, RunTrigger.Dependency, Times.t2, None)
        _          <- MembershipRun.complete(dependency, Times.t2.plusSeconds(60))
        _          <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t3, None)
        result     <- MembershipRun.selectLatestReported(clubId)
      } yield assertTrue(result.map(_.runId).contains(reported))
    }

  // Taking the later start, the next report would repeat what the run that saved last reported (#312).
  private def testSelectLatestReportedTakesTheLastToComplete =
    test("selectLatestReported returns the run that completed last, not the one that started last") {
      for {
        _             <- seedDb()
        _             <- deleteRuns
        startedFirst  <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t0, None)
        startedSecond <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t1, None)
        _             <- MembershipRun.complete(startedSecond, Times.t2)
        _             <- MembershipRun.complete(startedFirst, Times.t3)
        result        <- MembershipRun.selectLatestReported(clubId)
      } yield assertTrue(result.map(_.runId).contains(startedFirst))
    }

  // ==========================================================================
  // Suite G: classifyApiMembers (DB + fake HTTP)
  // ==========================================================================

  private def suiteClassifyApiMembers = suite("classifyApiMembers")(
    testUnchangedMemberMatchingSince,
    testDifferentSinceRejoined,
    testUsernameChangeSamePlayerId,
    testMemberHoldingNoNameListedUnderItsLastName,
    testNewPlayerNotInDb,
    testExistingPlayerJoinsClub,
    testExistingPlayerHoldingNoNameJoinsClub,
    testUsernameChangeAndStatusChange,
    testTrustModeKnownPlayerJoins,
    testTrustUsernamesFalseBypassesLookup
  )

  private def testUnchangedMemberMatchingSince = test("unchanged member — matching since") {
    val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
    val mem    = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t1)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
      membersByUsername = Map(Username("alice") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty
    )
    val apiMap = Map(Username("alice") -> Times.t1.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, storedRoster, Times.t2)
    } yield assertTrue(
      result.resolvedIds.contains(pid0),
      result.changes.isEmpty
    )
  }

  private def testDifferentSinceRejoined = test("different since → Rejoined, the old row ending at the new join") {
    val player       = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
    val mem          = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t0)
    val lastObserved = Times.t0.plusSeconds(3600)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid1 -> MemberState(player, mem)),
      membersByUsername = Map(Username("bob") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = Some(lastObserved),
      slugs = Set.empty
    )
    val apiMap = Map(Username("bob") -> Times.t1.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, storedRoster, Times.t2)
    } yield {
      val change = result.changes.head.changes.head
      assertTrue(
        result.resolvedIds.contains(pid1),
        result.changes.size == 1,
        // The API's join time, not the reconcile's, ends the old row.
        change == Rejoined(
          ts = Times.t1,
          previous = PreviousMembership.Recorded(lastSeen = lastObserved, until = Times.t1)
        ),
        result.closedMemberships == Chunk(mem.copy(lastSeen = Some(lastObserved), until = Some(Times.t1))),
        result.newMemberships == Chunk(ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t1))
      )
    }
  }

  private def testUsernameChangeSamePlayerId = test("username change — same player ID, different username") {
    val player = Player(pid2, Times.t0, Username("charlie-old"), Active, None, Times.t0)
    val mem    = ClubMember.joined(clubId = clubId, playerId = pid2, since = Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid2 -> MemberState(player, mem)),
      membersByUsername = Map(Username("charlie-old") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty
    )
    val apiMap    = Map(Username("charlie-new") -> Times.t0.getEpochSecond)
    val responses = Map("charlie-new" -> apiPlayerJson(102, "charlie-new"))

    for {
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, storedRoster, Times.t2)
    } yield assertTrue(
      result.resolvedIds.contains(pid2),
      result.changes.size == 1,
      result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
      result.updatedPlayers.nonEmpty
    )
  }

  // #254 step 5b: a member holding no name is absent from `membersByUsername`, which is keyed by held names, though its
  // row still stores the name it lost. Listed under that very name, it must be written back, not found unchanged.
  private def testMemberHoldingNoNameListedUnderItsLastName =
    test("a member holding no name, listed under the name its row stores, is updated to hold it") {
      val player = Player(pid2, Times.t0, Username("erin"), Active, None, Times.t0)
      val mem    = ClubMember.joined(clubId = clubId, playerId = pid2, since = Times.t0)
      val storedRoster = StoredRoster(
        membersByPlayerId = Map(pid2 -> MemberState(player, mem)),
        membersByUsername = Map.empty,
        unlistedByPlayerId = Map.empty,
        lastObservedAt = None,
        slugs = Set.empty
      )
      val apiMap    = Map(Username("erin") -> Times.t0.getEpochSecond)
      val responses = Map("erin" -> apiPlayerJson(PlayerId.unwrap(pid2), "erin"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, storedRoster, Times.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid2),
        result.updatedPlayers.map(p => (p.playerId, p.username)) == Chunk((pid2, Username("erin"))),
        result.changes.forall(_.changes.isEmpty)
      )
    }

  private def testNewPlayerNotInDb = test("new player — not in DB") {
    val apiMap    = Map(Username("diana") -> Times.t0.getEpochSecond)
    val responses = Map("diana" -> apiPlayerJson(103, "diana"))

    for {
      _      <- seedDb()
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, emptyStoredRoster, Times.t2)
    } yield {
      val change = result.changes.head.changes.head
      assertTrue(
        result.resolvedIds.contains(pid3),
        result.changes.size == 1,
        change.isInstanceOf[NewMember],
        change.timestamp == Times.t0, // API join time, not reconciliation time
        result.newPlayers.nonEmpty,
        result.newMemberships.nonEmpty
      )
    }
  }

  private def testExistingPlayerJoinsClub = test("existing player joins club") {
    val player4   = Player(pid4, Times.t0, Username("eve"), Active, None, Times.t0)
    val apiMap    = Map(Username("eve") -> Times.t1.getEpochSecond)
    val responses = Map("eve" -> apiPlayerJson(104, "eve"))

    for {
      _      <- seedDb(players = List(player4))
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, emptyStoredRoster, Times.t2)
    } yield {
      val change = result.changes.head.changes.head
      assertTrue(
        result.resolvedIds.contains(pid4),
        result.changes.size == 1,
        change.isInstanceOf[JoinedClub],
        change.timestamp == Times.t1, // API join time, not reconciliation time
        result.newMemberships.nonEmpty
      )
    }
  }

  // #254 step 5b: the same as a member holding no name, for a player outside the club, whose held name comes from
  // `player_name` rather than the roster matching.
  private def testExistingPlayerHoldingNoNameJoinsClub =
    test("existing player holding no name joins under the name its row stores, and is updated to hold it") {
      val nameless  = Player(pid4, Times.t0, Username("gail"), Active, None, Times.t0)
      val taker     = Player(pid3, Times.t0, Username("gail"), Active, None, Times.t0)
      val apiMap    = Map(Username("gail") -> Times.t1.getEpochSecond)
      val responses = Map("gail" -> apiPlayerJson(104, "gail"))

      for {
        _      <- seedDb(players = List(nameless))
        _      <- Player.insert(taker)
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, emptyStoredRoster, Times.t2)
      } yield assertTrue(
        result.updatedPlayers.map(p => (p.playerId, p.username)) == Chunk((pid4, Username("gail"))),
        result.changes.flatMap(_.changes) == Chunk(JoinedClub(Times.t1)),
        result.newMemberships.map(_.playerId) == Chunk(pid4)
      )
    }

  private def testUsernameChangeAndStatusChange = test("username change + status change") {
    val player = Player(pid5, Times.t0, Username("frank-old"), Active, None, Times.t0)
    val mem    = ClubMember.joined(clubId = clubId, playerId = pid5, since = Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid5 -> MemberState(player, mem)),
      membersByUsername = Map(Username("frank-old") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty
    )
    val apiMap    = Map(Username("frank-new") -> Times.t0.getEpochSecond)
    val responses = Map("frank-new" -> apiPlayerJson(105, "frank-new", status = "closed"))

    for {
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, storedRoster, Times.t2)
    } yield assertTrue(
      result.resolvedIds.contains(pid5),
      result.changes.size == 1,
      result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
      result.changes.head.changes.exists(_.isInstanceOf[StatusChange])
    )
  }

  private def testTrustModeKnownPlayerJoins = test("trust-mode: known player joins club without API call") {
    val player = Player(pid3, Times.t0, Username("diana"), Active, None, Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map.empty,
      membersByUsername = Map.empty,
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty,
      knownPlayersByUsername = Map(Username("diana") -> player)
    )
    val apiMap = Map(Username("diana") -> Times.t1.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, storedRoster, Times.t2)
    } yield {
      val change = result.changes.head.changes.head
      assertTrue(
        result.resolvedIds.contains(pid3),
        result.changes.size == 1,
        change.isInstanceOf[JoinedClub],
        change.timestamp == Times.t1, // API join time, not reconciliation time
        result.newMemberships.nonEmpty,
        result.updatedPlayers.isEmpty
      )
    }
  }

  private def testTrustUsernamesFalseBypassesLookup = test("trustUsernames=false bypasses known player lookup") {
    val player = Player(pid3, Times.t0, Username("diana"), Active, None, Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map.empty,
      membersByUsername = Map.empty,
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty,
      knownPlayersByUsername = Map(Username("diana") -> player)
    )
    val apiMap = Map(Username("diana") -> Times.t0.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(
        client,
        clubId,
        apiMap,
        storedRoster,
        Times.t2,
        trustUsernames = false
      ).exit
    } yield assertTrue(result.isFailure)
  }

  // ==========================================================================
  // Suite H: classifyDisappeared (fake HTTP)
  // ==========================================================================

  private def suiteClassifyDisappeared = suite("classifyDisappeared")(
    testActivePlayerLeftClub,
    testClosedAccountListingTheClubIsHidden,
    testClosedAccountNotListingTheClubLeftFirst,
    testClosedAccountWithoutAListIsHidden,
    testAClosureAnotherJobSawIsCheckedToo,
    testMatchRefFallbackRenamedStillInClub,
    testMatchRefFallbackRenamedLeftClub,
    testMatchRefFallbackRenamedClosed,
    testMatchRefFallbackRenamedReopened,
    testApi404Unresolvable,
    testDifferentPlayerIdUnresolvable,
    testAllResolvedEmptyResults
  )

  private def testActivePlayerLeftClub = test("active player left club → LeftClub with now timestamp") {
    val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    val mem    = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
      membersByUsername = Map(Username("alice") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty
    )
    val responses = Map("alice" -> apiPlayerJson(100, "alice"))

    for {
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyDisappeared(
        client,
        storedRoster,
        Set.empty,
        Map.empty,
        Times.t2
      )
    } yield {
      val change = result.changes.head.changes.head
      assertTrue(
        result.changes.size == 1,
        change.isInstanceOf[LeftClub],
        change.timestamp == Times.t2, // detection time — no authoritative departure time
        result.closedMemberships.nonEmpty,
        result.closedMemberships.head.until.contains(Times.t2)
      )
    }
  }

  /** Classifies bob, a member this roster no longer lists, whose account closed after a last visit at t1 and whose club
    * list is `clubsOption`, if Chess.com has one. `storedStatus` is the status we held for him.
    */
  private def classifyClosedBob(storedStatus: PlayerStatusCategory, clubsOption: Option[String]) = {
    val bob = Player(pid1, Times.t0, Username("bob"), storedStatus, None, Times.t0)
    val row = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid1 -> MemberState(bob, row)),
      membersByUsername = Map(Username("bob") -> MemberState(bob, row)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set(club.slug)
    )
    val profile = apiPlayerJson(
      playerId = 101,
      username = "bob",
      status = "closed",
      lastOnline = Some(Times.t1.getEpochSecond)
    )
    for {
      _ <- seedDb(players = List(bob), members = List(row))
      client <- fakeChessComClient(
        responses = Map("bob" -> profile),
        clubsResponses = clubsOption.map("bob" -> _).toMap
      )
      result <- MembershipClassify.classifyDisappeared(client, storedRoster, Set.empty, Map.empty, Times.t2)
    } yield (row, result)
  }

  private val keptClubs =
    connectZIO(sql"SELECT COUNT(*)::INT FROM player_club_list_entry WHERE player_id = $pid1".query[Int].run().head)

  private def testClosedAccountListingTheClubIsHidden =
    test("a member whose closed account still lists the club is hidden since its last visit, not ended") {
      for {
        (row, result) <- classifyClosedBob(Active, Some(clubListJson("test-club" -> Times.t0)))
        kept          <- keptClubs
      } yield assertTrue(
        result.changes.flatMap(_.changes) == Chunk(AccountClosed(Times.t1, Closed)),
        result.updatedPlayers.map(_.status) == Chunk(Closed),
        result.hiddenMemberships == Chunk(row.hidden(Times.t1)),
        result.closedMemberships.isEmpty,
        kept == 1
      )
    }

  private def testClosedAccountNotListingTheClubLeftFirst =
    test("a member whose closed account's list leaves the club out had left, and ends at this roster") {
      classifyClosedBob(Active, Some(clubListJson("other-club" -> Times.t0))).map { (row, result) =>
        assertTrue(
          result.changes.flatMap(_.changes) == Chunk(StatusChange(Times.t2, Active), LeftClub(Times.t2)),
          result.closedMemberships == Chunk(row.copy(lastSeen = Some(Times.t0), until = Some(Times.t2))),
          result.hiddenMemberships.isEmpty
        )
      }
    }

  /** An absence from the roster is not a leave (docs/chess-com-api.md, closed accounts). */
  private def testClosedAccountWithoutAListIsHidden =
    test("a member whose closed account has no club list to be had, or none readable, stays a member, hidden") {
      for {
        (row, absent)   <- classifyClosedBob(Active, None)
        (_, unreadable) <- classifyClosedBob(Active, Some("{}"))
      } yield assertTrue(
        absent.hiddenMemberships == Chunk(row.hidden(Times.t1)),
        unreadable.hiddenMemberships == Chunk(row.hidden(Times.t1)),
        (absent.closedMemberships ++ unreadable.closedMemberships).isEmpty
      )
    }

  private def testAClosureAnotherJobSawIsCheckedToo =
    test("a member whose closure another job recorded is checked against its club list like any other") {
      classifyClosedBob(Closed, Some(clubListJson("test-club" -> Times.t0))).map { (row, result) =>
        assertTrue(
          result.changes.flatMap(_.changes) == Chunk(AccountClosed(Times.t1, Closed)),
          result.updatedPlayers.isEmpty,
          result.hiddenMemberships == Chunk(row.hidden(Times.t1))
        )
      }
    }

  private val matchRefId = ClubMatchId(999000L)

  /** Runs classifyDisappeared for a player whose old username 404s and resolves via match-ref to "ed-new", with
    * `profile` and `clubLists` as Chess.com's answers under that name. Caller controls apiMap to simulate "still in
    * club under new name" vs "actually left".
    */
  private def runMatchRefFallbackRenamed(
    apiMap: Map[Username, Long],
    storedStatus: PlayerStatusCategory,
    profile: String,
    clubLists: Map[String, String]
  ) = {
    val player = Player(pid5, Times.t0, Username("ed-old"), storedStatus, None, Times.t0)
    val mem    = ClubMember.joined(clubId = clubId, playerId = pid5, since = Times.t0)
    val ref    = PlayerMatchRef(pid5, matchRefId, isLive = false, isTeam1 = true, boardIdx = 1)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid5 -> MemberState(player, mem)),
      membersByUsername = Map(Username("ed-old") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set(club.slug)
    )
    val responses = Map("ed-new" -> profile)
    val matchJson = apiDailyMatchJson(
      matchId = matchRefId.value,
      team1Club = "test-club",
      team2Club = "other-club",
      team1Players = List("ed-new" -> 1),
      team2Players = List("opponent" -> 1)
    )
    val matchResponses = Map(matchRefId.value.toString -> matchJson)
    // The resolver's Tier B board-endpoint trick fetches /pub/match/{id}/{board} and reads the surviving username
    // after eliminating the opposing side. Provide a fixture where ed-new (the renamed player) appears as team1's
    // board-1 occupant alongside `opponent`.
    val boardResponses = Map(
      (matchRefId.value.toString, "1") -> apiMatchBoardJson(
        matchId = matchRefId.value,
        board = 1,
        team1Username = "ed-new",
        team2Username = "opponent"
      )
    )

    for {
      _ <- seedDb(players = List(player), members = List(mem), matchRefs = List(ref))
      client <- fakeChessComClient(
        responses = responses,
        failures = Set("ed-old"),
        clubsResponses = clubLists,
        matchResponses = matchResponses,
        boardResponses = boardResponses
      )
      result <- MembershipClassify.classifyDisappeared(
        client,
        storedRoster,
        Set.empty,
        apiMap,
        Times.t2
      )
    } yield result
  }

  private val edNewProfile = apiPlayerJson(playerId = 105, username = "ed-new")

  private def testMatchRefFallbackRenamedStillInClub =
    test("matchRefFallback: renamed player still in club → no LeftClub, membership not closed") {
      runMatchRefFallbackRenamed(
        apiMap = Map(Username("ed-new") -> Times.t0.getEpochSecond),
        storedStatus = Active,
        profile = edNewProfile,
        clubLists = Map.empty
      ).map { result =>
        val changes = result.changes.head.changes
        assertTrue(
          result.changes.size == 1,
          changes.exists(_.isInstanceOf[UsernameChange]),
          !changes.exists(_.isInstanceOf[LeftClub]),
          result.updatedPlayers.head.username == Username("ed-new"),
          result.closedMemberships.isEmpty
        )
      }
    }

  private def testMatchRefFallbackRenamedLeftClub =
    test("matchRefFallback: renamed player no longer in club → LeftClub and membership closed") {
      runMatchRefFallbackRenamed(
        apiMap = Map.empty,
        storedStatus = Active,
        profile = edNewProfile,
        clubLists = Map.empty
      ).map { result =>
        val changes = result.changes.head.changes
        assertTrue(
          result.changes.size == 1,
          changes.exists(_.isInstanceOf[UsernameChange]),
          changes.exists(_.isInstanceOf[LeftClub]),
          result.updatedPlayers.head.username == Username("ed-new"),
          result.closedMemberships.nonEmpty,
          result.closedMemberships.head.until.contains(Times.t2)
        )
      }
    }

  /** Its list is fetched under the name the match led to, which its profile has just answered to. */
  private def testMatchRefFallbackRenamedClosed =
    test("matchRefFallback: renamed member whose closed account lists the club → hidden since its last visit") {
      val closed = apiPlayerJson(
        playerId = 105,
        username = "ed-new",
        status = "closed",
        lastOnline = Some(Times.t1.getEpochSecond)
      )
      runMatchRefFallbackRenamed(
        apiMap = Map.empty,
        storedStatus = Active,
        profile = closed,
        clubLists = Map("ed-new" -> clubListJson("test-club" -> Times.t0))
      ).map { result =>
        assertTrue(
          result.changes.flatMap(_.changes).toSet ==
            Set(UsernameChange(Times.t2, Username("ed-old")), AccountClosed(Times.t1, Closed)),
          result.hiddenMemberships.map(_.lastSeen) == Chunk(Some(Times.t1)),
          result.closedMemberships.isEmpty
        )
      }
    }

  private def testMatchRefFallbackRenamedReopened =
    test("matchRefFallback: renamed member whose closed account reopened, still in club → StatusChange, row kept") {
      runMatchRefFallbackRenamed(
        apiMap = Map(Username("ed-new") -> Times.t0.getEpochSecond),
        storedStatus = Closed,
        profile = edNewProfile,
        clubLists = Map.empty
      ).map { result =>
        assertTrue(
          result.changes.flatMap(_.changes).toSet ==
            Set(UsernameChange(Times.t2, Username("ed-old")), StatusChange(Times.t2, Closed)),
          result.closedMemberships.isEmpty,
          result.hiddenMemberships.isEmpty
        )
      }
    }

  private def testApi404Unresolvable = test("API 404 → Unresolvable") {
    val player = Player(pid2, Times.t0, Username("charlie"), Active, None, Times.t0)
    val mem    = ClubMember.joined(clubId = clubId, playerId = pid2, since = Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid2 -> MemberState(player, mem)),
      membersByUsername = Map(Username("charlie") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty
    )

    for {
      client <- fakeChessComClient(Map.empty, failures = Set("charlie"))
      result <- MembershipClassify.classifyDisappeared(
        client,
        storedRoster,
        Set.empty,
        Map.empty,
        Times.t2
      )
    } yield assertTrue(
      result.changes.size == 1,
      result.changes.head.changes.exists(_.isInstanceOf[Unresolvable]),
      result.closedMemberships.nonEmpty
    )
  }

  private def testDifferentPlayerIdUnresolvable = test("different player ID at same username → Unresolvable") {
    val player = Player(pid3, Times.t0, Username("diana"), Active, None, Times.t0)
    val mem    = ClubMember.joined(clubId = clubId, playerId = pid3, since = Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid3 -> MemberState(player, mem)),
      membersByUsername = Map(Username("diana") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty
    )
    val responses = Map("diana" -> apiPlayerJson(999, "diana"))

    for {
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyDisappeared(
        client,
        storedRoster,
        Set.empty,
        Map.empty,
        Times.t2
      )
    } yield assertTrue(
      result.changes.size == 1,
      result.changes.head.changes.exists(_.isInstanceOf[Unresolvable]),
      result.closedMemberships.nonEmpty
    )
  }

  private def testAllResolvedEmptyResults = test("all resolved → empty results") {
    val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    val mem    = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
    val storedRoster = StoredRoster(
      membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
      membersByUsername = Map(Username("alice") -> MemberState(player, mem)),
      unlistedByPlayerId = Map.empty,
      lastObservedAt = None,
      slugs = Set.empty
    )

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyDisappeared(
        client,
        storedRoster,
        Set(pid0),
        Map.empty,
        Times.t2
      )
    } yield assertTrue(
      result.changes.isEmpty,
      result.updatedPlayers.isEmpty,
      result.closedMemberships.isEmpty
    )
  }

  // ==========================================================================
  // Suite I: reconcile (end-to-end)
  // ==========================================================================

  private def suiteReconcile = suite("reconcile (end-to-end)")(
    testDeltaReflectsNewJoinAcrossRuns,
    testAbortsWhenSlugAnswersAsAnotherClub,
    testReportRefusesAContestedName,
    testALeaveEndsBetweenTwoRosters,
    testRejoinPersistsBothRows,
    testJoinsADependencySawAreReportedNext,
    testAReconcileOverlappingAnotherBuildsOnItsSave,
    testStaleCheckIgnoresAFailedRun,
    testARunThatCannotCompleteSavesNothing,
    testAHiddenMemberListedAgainReopens,
    testARowEndedAtAClosureReopens,
    testAHiddenMemberWhoRejoinedHadLeft,
    testAClosedAccountsListHidesItsEndedRow,
    testAClosedAccountsListOpensTheMembershipItShows,
    testANameThatNoLongerAnswersIsReportedOnce,
    testAListOverlappingARecordedMembershipSettlesNothing,
    testAClosedAccountThatPlayedForTheClubIsAsked,
    testANameAnsweringForAnotherPlayerSettlesNothing,
    testAReactivatedAccountListedAgainReopens,
    testAReactivatedAccountMissingFromTheRosterHasLeft,
    testAReactivatedAccountUnderANewNameReopens
  )

  /** Chess.com for the test club: `roster` as its members with when each joined, and `profiles` as the players it
    * knows by name.
    */
  private def clubRoutes(roster: List[(String, Instant)], profiles: Map[String, String]): Routes[Any, Response] =
    clubRoutesRunning(roster = roster, profiles = profiles, clubLists = Map.empty, onRosterFetch = ZIO.unit)

  /** [[clubRoutes]], with `clubLists` as the club lists of the players it knows by name. */
  private def clubRoutesListing(
    roster: List[(String, Instant)],
    profiles: Map[String, String],
    clubLists: Map[String, String]
  ): Routes[Any, Response] =
    clubRoutesRunning(roster = roster, profiles = profiles, clubLists = clubLists, onRosterFetch = ZIO.unit)

  /** [[clubRoutesListing]], running `onRosterFetch` before it answers each fetch of the roster. */
  private def clubRoutesRunning(
    roster: List[(String, Instant)],
    profiles: Map[String, String],
    clubLists: Map[String, String],
    onRosterFetch: UIO[Any]
  ): Routes[Any, Response] = {
    val notFound = Response.json("""{"code": 0, "message": "Resource \"\" not found."}""").status(Status.NotFound)
    Routes(
      Method.GET / "pub" / "club" / string("slug") / "members" -> handler { (_: String, _: Request) =>
        onRosterFetch.as(Response.json(apiClubMembersJson(roster.map((name, joined) => (name, joined.getEpochSecond)))))
      },
      Method.GET / "pub" / "club" / string("slug") -> handler { (slug: String, _: Request) =>
        Response.json(apiClubJson(ClubId.unwrap(clubId), slug))
      },
      Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
        profiles.get(username).fold(notFound)(Response.json(_))
      },
      Method.GET / "pub" / "player" / string("username") / "clubs" -> handler { (username: String, _: Request) =>
        clubLists.get(username).fold(notFound)(Response.json(_))
      }
    )
  }

  private def reconcileWith(routes: Routes[Any, Response], trigger: RunTrigger) =
    TestChessComClientSupport.fakeClient(routes).flatMap { client =>
      MembershipApp
        .reconcile(clubSlug = ClubSlug("test-club"), expectedClubIdOption = None, trigger = trigger)
        .provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
    }

  private def completedRun(startedAt: Instant, completedAt: Instant) =
    MembershipRun.insert(clubId, RunTrigger.Cli, startedAt, None).flatMap(MembershipRun.complete(_, completedAt))

  private val deleteRuns = connectZIO(sql"DELETE FROM membership_run WHERE club_id = $clubId".update.run())

  private def testALeaveEndsBetweenTwoRosters =
    test("a member a roster no longer lists was last seen at the previous roster and gone at this one") {
      val alice        = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val aliceRow     = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
      val lastObserved = Times.t0.plusSeconds(3600)
      val profiles     = Map("alice" -> apiPlayerJson(playerId = PlayerId.unwrap(pid0), username = "alice"))
      for {
        _        <- seedDb(players = List(alice), members = List(aliceRow))
        _        <- ClubRosterCoverage.observe(clubId, lastObserved)
        before   <- ZIO.succeed(Instant.now())
        _        <- reconcileWith(clubRoutes(roster = Nil, profiles = profiles), RunTrigger.Cli)
        after    <- ZIO.succeed(Instant.now())
        rows     <- ClubMember.selectClub(clubId)
        coverage <- ClubRosterCoverage.select(clubId)
      } yield assertTrue(
        rows.map(_.lastSeen) == List(Some(lastObserved)),
        rows.flatMap(_.until).exists(until => !until.isBefore(before) && !until.isAfter(after)),
        coverage.map(_.firstObservedAt).contains(lastObserved),
        coverage.map(_.lastObservedAt) == rows.flatMap(_.until).headOption
      )
    }

  // The new row would overlap the old one if it opened before the old one closed.
  private def testRejoinPersistsBothRows =
    test("a rejoin closes the old row at the new join and opens the new one") {
      val bob          = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
      val oldRow       = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t0)
      val lastObserved = Times.t0.plusSeconds(3600)
      for {
        _    <- seedDb(players = List(bob), members = List(oldRow))
        _    <- ClubRosterCoverage.observe(clubId, lastObserved)
        _    <- reconcileWith(clubRoutes(roster = List("bob" -> Times.t1), profiles = Map.empty), RunTrigger.Cli)
        rows <- ClubMember.selectClub(clubId)
      } yield assertTrue(
        rows.toSet == Set(
          oldRow.copy(lastSeen = Some(lastObserved), until = Some(Times.t1)),
          ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t1)
        )
      )
    }

  private def testJoinsADependencySawAreReportedNext =
    test("a join a reconcile run by another job saw is reported by the next membership run") {
      val alice    = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val aliceRow = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
      val routes = clubRoutes(
        roster = List("alice" -> Times.t0, "bob" -> Times.t1),
        profiles = Map("bob" -> apiPlayerJson(playerId = PlayerId.unwrap(pid1), username = "bob"))
      )
      for {
        _      <- deleteRuns
        _      <- seedDb(players = List(alice), members = List(aliceRow))
        _      <- completedRun(startedAt = Times.t0, completedAt = Times.t0.plusSeconds(60))
        _      <- reconcileWith(routes, RunTrigger.Dependency)
        result <- reconcileWith(routes, RunTrigger.Cli)
      } yield assertTrue(
        result.changes.map(c => (c.playerId, c.changes.toList)) == Chunk((pid1, List(JoinedClub(Times.t1)))),
        result.previousMemberCount == 1,
        result.currentMemberCount == 2
      )
    }

  // #312: the later of two overlapping reconciles inserted the member the earlier had saved, and failed on the key.
  private def testAReconcileOverlappingAnotherBuildsOnItsSave =
    test("a reconcile another one saved during fetches the roster again and saves against what that one saved") {
      val alice    = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val aliceRow = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
      val bobRow   = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t1)
      val completedRuns = connectZIO {
        sql"SELECT COUNT(*)::INT FROM membership_run WHERE club_id = $clubId AND completed_at IS NOT NULL"
          .query[Int].run().head
      }
      for {
        _             <- deleteRuns
        _             <- seedDb(players = List(alice), members = List(aliceRow))
        _             <- ClubRosterCoverage.observe(clubId, Times.t0)
        _             <- completedRun(startedAt = Times.t0, completedAt = Times.t0.plusSeconds(60))
        reached       <- Promise.make[Nothing, Unit]
        gate          <- Promise.make[Nothing, Unit]
        rosterFetches <- Ref.make(0)
        // The first fetch answers only after the other reconcile saves, so the version the first read is older than
        // that save, whether the rows it loaded alongside are or not.
        holdFirst = rosterFetches.updateAndGet(_ + 1).flatMap { fetches =>
          ZIO.whenDiscard(fetches == 1)(reached.succeed(()) *> gate.await)
        }
        routes = clubRoutesRunning(
          roster = List("alice" -> Times.t0, "bob" -> Times.t1),
          profiles = Map("bob" -> apiPlayerJson(playerId = PlayerId.unwrap(pid1), username = "bob")),
          clubLists = Map.empty,
          onRosterFetch = holdFirst
        )
        first   <- reconcileWith(routes, RunTrigger.Cli).fork
        _       <- reached.await
        _       <- reconcileWith(routes, RunTrigger.Dependency)
        _       <- gate.succeed(())
        result  <- first.join
        rows    <- ClubMember.selectClub(clubId)
        fetches <- rosterFetches.get
        runs    <- completedRuns
      } yield assertTrue(
        rows.toSet == Set(aliceRow, bobRow),
        fetches == 3,
        runs == 3,
        result.changes.map(c => (c.playerId, c.changes.toList)) == Chunk((pid1, List(JoinedClub(Times.t1))))
      )
    }

  private def testStaleCheckIgnoresAFailedRun =
    test("a report reconciles first when the only run newer than its end failed") {
      val completedRuns =
        connectZIO(sql"SELECT COUNT(*)::INT FROM membership_run WHERE completed_at IS NOT NULL".query[Int].run().head)
      for {
        _      <- deleteRuns
        _      <- seedDb()
        _      <- completedRun(startedAt = Times.t0, completedAt = Times.t0.plusSeconds(60))
        _      <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t3, None)
        before <- completedRuns
        client <- TestChessComClientSupport.fakeClient(clubRoutes(roster = Nil, profiles = Map.empty))
        _ <- MembershipApp
          .reconcileIfStale(clubSlug = ClubSlug("test-club"), until = Times.t2)
          .provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
        after <- completedRuns
      } yield assertTrue(after == before + 1)
    }

  // Otherwise the stale check and the report's baseline would measure from a run older than the rows.
  private def testARunThatCannotCompleteSavesNothing =
    test("a reconcile that cannot mark its run complete saves nothing its roster implied") {
      val alice    = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val aliceRow = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
      val profiles = Map("alice" -> apiPlayerJson(playerId = PlayerId.unwrap(pid0), username = "alice"))
      val refuseCompletion = connectZIO {
        sql"""CREATE OR REPLACE FUNCTION refuse_completion() RETURNS trigger LANGUAGE plpgsql
              AS 'BEGIN RAISE EXCEPTION ''completion refused''; END'""".update.run()
        sql"DROP TRIGGER IF EXISTS refuse_completion ON membership_run".update.run()
        sql"""CREATE TRIGGER refuse_completion BEFORE UPDATE OF completed_at ON membership_run
              FOR EACH ROW EXECUTE FUNCTION refuse_completion()""".update.run()
      }
      val allowCompletion = connectZIO {
        sql"DROP TRIGGER IF EXISTS refuse_completion ON membership_run".update.run()
        sql"DROP FUNCTION IF EXISTS refuse_completion()".update.run()
      }
      for {
        _ <- seedDb(players = List(alice), members = List(aliceRow))
        exit <- ZIO.acquireReleaseWith(refuseCompletion)(_ => allowCompletion.orDie) { _ =>
          reconcileWith(clubRoutes(roster = Nil, profiles = profiles), RunTrigger.Cli).exit
        }
        rows     <- ClubMember.selectClub(clubId)
        coverage <- ClubRosterCoverage.select(clubId)
      } yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.getMessage.contains("completion refused")),
        rows == List(aliceRow),
        coverage.isEmpty
      )
    }

  // A name two clubs held and nobody holds now is the one thing resolution won't guess at, so the report must say so
  // rather than reconcile by name and surface Chess.com's 404 instead.
  private def testReportRefusesAContestedName =
    test("a report on a name several clubs held fails with the ambiguity, reconciling nothing") {
      val gone = """{"code": 0, "message": "Resource \"\" not found."}"""
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "club" / string("slug") -> handler { (_: String, _: Request) =>
          Response.json(gone).status(Status.NotFound)
        }
      )
      val holders = List((ClubId(7_801), "report-a-now"), (ClubId(7_802), "report-b-now"))
      for {
        _ <- ZIO.foreachDiscard(holders) { case (id, now) =>
          Club.upsert(Club(id, Times.t0, ClubSlug("contested-report"), "Contested", None, None, None)) *>
            Club.upsert(Club(id, Times.t0, ClubSlug(now), "Contested", None, None, None))
        }
        client     <- TestChessComClientSupport.fakeClient(routes)
        runsBefore <- connectZIO(sql"SELECT COUNT(*)::INT FROM membership_run".query[Int].run().head)
        exit <- MembershipApp.reconcileIfStale(ClubSlug("contested-report"), Times.t1)
          .provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
          .exit
        runsAfter <- connectZIO(sql"SELECT COUNT(*)::INT FROM membership_run".query[Int].run().head)
      } yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.getMessage.contains("is ambiguous")),
        runsAfter == runsBefore
      )
    }

  private def testAbortsWhenSlugAnswersAsAnotherClub =
    test("a slug that now answers as a different club aborts before writing anything") {
      val expected = ClubId(ClubId.unwrap(clubId) + 1000)
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "club" / string("slug") -> handler { (slug: String, _: Request) =>
          Response.json(apiClubJson(ClubId.unwrap(clubId), slug))
        }
      )
      for {
        client     <- TestChessComClientSupport.fakeClient(routes)
        runsBefore <- connectZIO(sql"SELECT COUNT(*)::INT FROM membership_run".query[Int].run().head)
        exit <- MembershipApp.reconcile(ClubSlug("test-club"), expectedClubIdOption = Some(expected))
          .provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
          .exit
        runsAfter <- connectZIO(sql"SELECT COUNT(*)::INT FROM membership_run".query[Int].run().head)
      } yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists(_.isInstanceOf[ClubMismatchException]),
        runsAfter == runsBefore
      )
    }

  private def testDeltaReflectsNewJoinAcrossRuns =
    test("previousMemberCount ignores rows this run inserts (regression)") {
      // Regression: previousMemberCount used to be sampled after this run completed itself,
      // so selectLatestCompleted returned THIS run and the delta always collapsed to 0.
      val bobJoined       = Times.t1.minusSeconds(600) // before the prior run completed
      val alice           = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val aliceMembership = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
      val routes = clubRoutes(
        roster = List("alice" -> Times.t0, "bob" -> bobJoined),
        profiles = Map(
          "alice" -> apiPlayerJson(playerId = PlayerId.unwrap(pid0), username = "alice"),
          "bob" -> apiPlayerJson(
            playerId = PlayerId.unwrap(pid1),
            username = "bob",
            joined = bobJoined.getEpochSecond
          )
        )
      )
      for {
        _      <- deleteRuns
        _      <- seedDb(players = List(alice), members = List(aliceMembership))
        _      <- completedRun(startedAt = Times.t0, completedAt = Times.t1)
        result <- reconcileWith(routes, RunTrigger.Cli)
      } yield assertTrue(
        result.newMemberships.size == 1,
        result.previousMemberCount == 1,
        result.currentMemberCount == 2
      )
    }

  // --- Closed accounts (ADR 0022) ---

  private def endedRow(playerId: PlayerId, since: Instant, until: Instant) = ClubMember(
    clubId = clubId,
    playerId = playerId,
    since = since,
    lastSeen = Some(since),
    until = Some(until)
  )

  private def closedProfile(playerId: PlayerId, username: String, lastOnline: Instant) = apiPlayerJson(
    playerId = PlayerId.unwrap(playerId),
    username = username,
    status = "closed",
    lastOnline = Some(lastOnline.getEpochSecond)
  )

  /** A second row since the same join would break the key, and every later reconcile of the club with it (#311). */
  private def testAHiddenMemberListedAgainReopens =
    test("a member a closed account hid, listed again since the same join, reopens rather than joining again") {
      val bob    = Player(pid1, Times.t0, Username("bob"), Closed, None, Times.t0)
      val hidden = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t0).hidden(Times.t1)
      val routes = clubRoutes(
        roster = List("bob" -> Times.t0),
        profiles = Map("bob" -> apiPlayerJson(playerId = PlayerId.unwrap(pid1), username = "bob"))
      )
      for {
        _      <- seedDb(players = List(bob), members = List(hidden))
        result <- reconcileWith(routes, RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
        player <- Player.selectId(pid1)
      } yield assertTrue(
        rows == List(ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t0)),
        result.reopenedMemberships.size == 1,
        result.newMemberships.isEmpty,
        player.map(_.status).contains(Active)
      )
    }

  /** A row ended at its account's closure would wedge the same way. The roster, not the list kept while the account was
    * closed, settles a member it lists.
    */
  private def testARowEndedAtAClosureReopens =
    test("a row a closure ended reopens when the roster lists its join again, whatever the list kept says") {
      val carol = Player(pid2, Times.t0, Username("carol"), Closed, None, Times.t1)
      val ended = endedRow(playerId = pid2, since = Times.t0, until = Times.t1)
      val kept = PlayerClubList(
        playerId = pid2,
        fetchedAt = Times.t2,
        lastOnlineOption = Some(Times.t1),
        clubs = Map(club.slug -> Times.t0)
      )
      val routes = clubRoutes(
        roster = List("carol" -> Times.t0),
        profiles = Map("carol" -> apiPlayerJson(playerId = PlayerId.unwrap(pid2), username = "carol"))
      )
      for {
        _    <- seedDb(players = List(carol), members = List(ended))
        _    <- PlayerClubList.save(kept)
        _    <- reconcileWith(routes, RunTrigger.Cli)
        rows <- ClubMember.selectClub(clubId)
      } yield assertTrue(rows == List(ClubMember.joined(clubId = clubId, playerId = pid2, since = Times.t0)))
    }

  /** Its account is active again, so the roster's player is matched by name without asking Chess.com. */
  private def testAHiddenMemberWhoRejoinedHadLeft =
    test("a member a closed account hid, listed since a later join, had left: the hidden row ends at that join") {
      val dave   = Player(pid3, Times.t0, Username("dave"), Active, None, Times.t0)
      val hidden = ClubMember.joined(clubId = clubId, playerId = pid3, since = Times.t0).hidden(Times.t1)
      for {
        _      <- seedDb(players = List(dave), members = List(hidden))
        result <- reconcileWith(clubRoutes(roster = List("dave" -> Times.t2), profiles = Map.empty), RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
      } yield assertTrue(
        rows.toSet == Set(
          hidden.copy(until = Some(Times.t2)),
          ClubMember.joined(clubId = clubId, playerId = pid3, since = Times.t2)
        ),
        result.changes.flatMap(_.changes) == Chunk(
          Rejoined(ts = Times.t2, previous = PreviousMembership.Recorded(lastSeen = Times.t1, until = Times.t2))
        )
      )
    }

  private def testAClosedAccountsListHidesItsEndedRow =
    test("a closed account whose list shows the club since a row's start has the row hidden") {
      val erin  = Player(pid4, Times.t0, Username("erin"), Closed, None, Times.t1)
      val ended = endedRow(playerId = pid4, since = Times.t0, until = Times.t1)
      val routes = clubRoutesListing(
        roster = Nil,
        profiles = Map("erin" -> closedProfile(playerId = pid4, username = "erin", lastOnline = Times.t2)),
        clubLists = Map("erin" -> clubListJson("test-club" -> Times.t0))
      )
      for {
        _      <- seedDb(players = List(erin), members = List(ended))
        result <- reconcileWith(routes, RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
      } yield assertTrue(
        rows == List(ended.hidden(Times.t2)),
        result.hiddenMemberships == Chunk(ended.hidden(Times.t2)),
        result.unsettledLists.isEmpty
      )
    }

  private def testAClosedAccountsListOpensTheMembershipItShows =
    test("a closed account whose club list shows a membership no row records has it opened hidden, after an earlier") {
      val finn    = Player(pid5, Times.t0, Username("finn"), Closed, None, Times.t1)
      val earlier = endedRow(playerId = pid5, since = Times.t0.minusSeconds(864_000), until = Times.t0.minusSeconds(1))
      val routes = clubRoutesListing(
        roster = Nil,
        profiles = Map("finn" -> closedProfile(playerId = pid5, username = "finn", lastOnline = Times.t3)),
        clubLists = Map("finn" -> clubListJson("test-club" -> Times.t0, "other-club" -> Times.t1))
      )
      for {
        _    <- seedDb(players = List(finn), members = List(earlier))
        _    <- reconcileWith(routes, RunTrigger.Cli)
        rows <- ClubMember.selectClub(clubId)
      } yield {
        val listed = ClubMember.joined(clubId = clubId, playerId = pid5, since = Times.t0).hidden(Times.t3)
        assertTrue(rows.toSet == Set(earlier, listed))
      }
    }

  private def testANameThatNoLongerAnswersIsReportedOnce =
    test("a closed account whose name no longer answers is reported once, its row left as it was") {
      val gina   = Player(pid0, Times.t0, Username("gina"), Closed, None, Times.t1)
      val ended  = endedRow(playerId = pid0, since = Times.t0, until = Times.t1)
      val routes = clubRoutesListing(roster = Nil, profiles = Map.empty, clubLists = Map.empty)
      for {
        _      <- seedDb(players = List(gina), members = List(ended))
        first  <- reconcileWith(routes, RunTrigger.Cli)
        second <- reconcileWith(routes, RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
      } yield assertTrue(
        first.unsettledLists.map(_.username) == Chunk(Username("gina")),
        second.unsettledLists.isEmpty,
        rows == List(ended)
      )
    }

  private def testAListOverlappingARecordedMembershipSettlesNothing =
    test("a closed account whose list shows a join inside a recorded membership is reported, its rows left alone") {
      val hank  = Player(pid1, Times.t0, Username("hank"), Closed, None, Times.t2)
      val ended = endedRow(playerId = pid1, since = Times.t1, until = Times.t2)
      val routes = clubRoutesListing(
        roster = Nil,
        profiles = Map("hank" -> closedProfile(playerId = pid1, username = "hank", lastOnline = Times.t3)),
        clubLists = Map("hank" -> clubListJson("test-club" -> Times.t0))
      )
      for {
        _      <- seedDb(players = List(hank), members = List(ended))
        result <- reconcileWith(routes, RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
      } yield assertTrue(result.unsettledLists.map(_.username) == Chunk(Username("hank")), rows == List(ended))
    }

  /** A closed account linked by a board alone is asked only if it played on the club's side. */
  private def testAClosedAccountThatPlayedForTheClubIsAsked =
    test("a closed account that played for the club, with no row, has its list read; one that played against it not") {
      val ivan = Player(pid2, Times.t0, Username("ivan"), Closed, None, Times.t1)
      val jack = Player(pid3, Times.t0, Username("jack"), Closed, None, Times.t1)
      val routes = clubRoutesListing(
        roster = Nil,
        profiles = Map(
          "ivan" -> closedProfile(playerId = pid2, username = "ivan", lastOnline = Times.t2),
          "jack" -> closedProfile(playerId = pid3, username = "jack", lastOnline = Times.t2)
        ),
        clubLists = Map(
          "ivan" -> clubListJson("test-club" -> Times.t0),
          "jack" -> clubListJson("test-club" -> Times.t0)
        )
      )
      val board = connectZIO {
        sql"""INSERT INTO club_match (match_id, name, status, time_class, boards, team1_club_id, team1_score_x2,
                team2_club_id, team2_score_x2, fetched_at)
              VALUES (931001, 'A match', 'Finished', 'Daily', 1, $clubId, 2, NULL, 2, now())""".update.run()
        sql"""INSERT INTO club_match_board (match_id, board, team1_player_id, team1_fair_play, team2_player_id,
                team2_fair_play, team1_score_x2, team2_score_x2)
              VALUES (931001, 1, $pid2, false, $pid3, false, 2, 2)""".update.run()
      }
      (for {
        _     <- seedDb(players = List(ivan, jack))
        _     <- board
        _     <- reconcileWith(routes, RunTrigger.Cli)
        rows  <- ClubMember.selectClub(clubId)
        asked <- connectZIO(sql"SELECT player_id FROM player_club_list".query[PlayerId].run().toSet)
      } yield assertTrue(
        rows == List(ClubMember.joined(clubId = clubId, playerId = pid2, since = Times.t0).hidden(Times.t2)),
        asked == Set(pid2)
      )).ensuring(TestDbCleanup.clearMatches.orDie)
    }

  /** A club list carries no player id, so the list under a name that passed to another account is never asked for. */
  private def testANameAnsweringForAnotherPlayerSettlesNothing =
    test("a closed account whose name now answers for another player is reported, its row left as it was") {
      val nora  = Player(pid3, Times.t0, Username("nora"), Closed, None, Times.t1)
      val ended = endedRow(playerId = pid3, since = Times.t0, until = Times.t1)
      val routes = clubRoutesListing(
        roster = Nil,
        profiles = Map("nora" -> closedProfile(playerId = PlayerId(931_311), username = "nora", lastOnline = Times.t2)),
        clubLists = Map("nora" -> clubListJson("test-club" -> Times.t0))
      )
      for {
        _      <- seedDb(players = List(nora), members = List(ended))
        result <- reconcileWith(routes, RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
      } yield assertTrue(result.unsettledLists.map(_.username) == Chunk(Username("nora")), rows == List(ended))
    }

  /** Another job saw the account active again, so the roster should list the row its closure hid. */
  private def testAReactivatedAccountListedAgainReopens =
    test("a member a closed account hid, active again and listed since the same join, reopens") {
      val olga   = Player(pid4, Times.t0, Username("olga"), Active, None, Times.t2)
      val hidden = ClubMember.joined(clubId = clubId, playerId = pid4, since = Times.t0).hidden(Times.t1)
      for {
        _      <- seedDb(players = List(olga), members = List(hidden))
        result <- reconcileWith(clubRoutes(roster = List("olga" -> Times.t0), profiles = Map.empty), RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
      } yield assertTrue(
        rows == List(ClubMember.joined(clubId = clubId, playerId = pid4, since = Times.t0)),
        result.reopenedMemberships.size == 1
      )
    }

  private def testAReactivatedAccountMissingFromTheRosterHasLeft =
    test("a member a closed account hid, active again but missing from the roster, has left since last seen") {
      val kate   = Player(pid0, Times.t0, Username("kate"), Active, None, Times.t2)
      val hidden = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0).hidden(Times.t1)
      val routes = clubRoutes(
        roster = Nil,
        profiles = Map("kate" -> apiPlayerJson(playerId = PlayerId.unwrap(pid0), username = "kate"))
      )
      for {
        _      <- seedDb(players = List(kate), members = List(hidden))
        result <- reconcileWith(routes, RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
      } yield assertTrue(
        rows.map(_.lastSeen) == List(Some(Times.t1)),
        rows.forall(_.until.nonEmpty),
        result.changes.flatMap(_.changes).exists(_.isInstanceOf[LeftClub]),
        result.currentMemberCount == 0
      )
    }

  private def testAReactivatedAccountUnderANewNameReopens =
    test("a member a closed account hid, active again and listed under a new name since the same join, reopens") {
      val lena   = Player(pid1, Times.t0, Username("lena"), Active, None, Times.t2)
      val hidden = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t0).hidden(Times.t1)
      val routes = clubRoutes(
        roster = List("lena-new" -> Times.t0),
        profiles = Map("lena-new" -> apiPlayerJson(playerId = PlayerId.unwrap(pid1), username = "lena-new"))
      )
      for {
        _      <- seedDb(players = List(lena), members = List(hidden))
        result <- reconcileWith(routes, RunTrigger.Cli)
        rows   <- ClubMember.selectClub(clubId)
      } yield assertTrue(
        rows == List(ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t0)),
        result.changes.flatMap(_.changes).exists(_.isInstanceOf[UsernameChange])
      )
    }

  // ==========================================================================
  // Suite: report (DB)
  // ==========================================================================

  private def suiteReport = suite("report (DB)")(
    testReportReadsStatusAndNameHistories,
    testReportInfersFromBoardsAndCountsFromRows
  )

  // The report reads statuses from `player_snapshot` and `player`, and names from `player_name` (#254 step 5b): a
  // closure followed by a rename keeps the closure's date, and the rename is dated when it was recorded.
  private def testReportReadsStatusAndNameHistories =
    test("report dates a status change and a later rename each from its own history") {
      val member  = Player(pid0, Times.t0, Username("hal-old"), Active, None, Times.t0)
      val renamed = Username("hal-new")
      for {
        _ <- seedDb(
          players = List(member),
          members = List(ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t1))
        )
        _         <- withTransaction(PlayerUpdater.archiveAndUpdate(member, member.username, Closed, None, Times.t2))
        closed    <- Player.selectId(pid0).someOrFailException
        _         <- withTransaction(PlayerUpdater.archiveAndUpdate(closed, renamed, Closed, None, Times.t3))
        renamedAt <- PlayerName.selectPlayer(pid0).map(_.last.since)
        result    <- MembershipReport.report(NamedClub(clubId, club.slug), Times.t0, renamedAt.plusSeconds(1))
      } yield assertTrue(
        result.summaries.map(_.username) == List(renamed),
        result.summaries.flatMap(_.changes) == List(
          JoinedClub(Times.t1),
          StatusChange(Times.t2, Active),
          UsernameChange(renamedAt, member.username)
        )
      )
    }

  /** Kim played twice for the club before joining it, Lee's account closed while a member, and Mo played for it once
    * before the range.
    */
  private def testReportInfersFromBoardsAndCountsFromRows =
    test("report lists a membership its boards show, a rejoin after it, and counts each change its events give") {
      val kim    = Player(pid1, Times.t0, Username("kim"), Active, None, Times.t0)
      val lee    = Player(pid4, Times.t0, Username("lee"), Closed, None, Times.t2)
      val mo     = Player(pid5, Times.t0, Username("mo"), Active, None, Times.t0)
      val kimRow = ClubMember.joined(clubId = clubId, playerId = pid1, since = Times.t2)
      val leeRow = ClubMember.joined(clubId = clubId, playerId = pid4, since = Times.t0).hidden(Times.t2)
      val played = List(Times.t1, Times.t1.plusSeconds(3600))
      val boards = played.map(pid1 -> _) :+ (pid5 -> Times.t0)
      val matches = connectZIO {
        boards.zipWithIndex.foreach { case ((playerId, startTime), i) =>
          val matchId = 931_101L + i
          sql"""INSERT INTO club_match (match_id, name, status, time_class, start_time, boards, team1_club_id,
                  team1_score_x2, team2_club_id, team2_score_x2, fetched_at)
                VALUES ($matchId, 'A match', 'Finished', 'Daily', $startTime, 1, $clubId, 2, NULL, 2, now())"""
            .update.run()
          sql"""INSERT INTO club_match_board (match_id, board, team1_player_id, team1_fair_play, team2_player_id,
                  team2_fair_play, team1_score_x2, team2_score_x2)
                VALUES ($matchId, 1, $playerId, false, NULL, false, 2, 2)""".update.run()
        }
      }
      (for {
        _      <- seedDb(players = List(kim, lee, mo), members = List(kimRow, leeRow))
        _      <- matches
        result <- MembershipReport.report(NamedClub(clubId, club.slug), Times.t1, Times.t3)
      } yield {
        val previous = PreviousMembership.Inferred(firstMatch = played.head, lastMatch = played.last)
        assertTrue(
          result.inferred == List(
            InferredMember(
              username = Username("kim"),
              membership = InferredMembership(
                playerId = pid1,
                firstMatch = played.head,
                lastMatch = played.last,
                matches = 2
              )
            )
          ),
          result.summaries.flatMap(_.changes).toSet ==
            Set(Rejoined(ts = Times.t2, previous = previous), AccountClosed(Times.t2, Closed)),
          result.memberCountAtStart == 1,
          result.memberCountAtEnd == 1
        )
      }).ensuring(TestDbCleanup.clearMatches.orDie)
    }
}
