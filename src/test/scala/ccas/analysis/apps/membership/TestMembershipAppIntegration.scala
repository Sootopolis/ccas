package ccas.analysis.apps.membership

import com.augustnagro.magnum.sql
import zio.{Chunk, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.apps.membership.MembershipClassify.{PhaseBResult, PhaseCResult}
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{
  apiClubJson,
  apiClubMembersJson,
  apiDailyMatchJson,
  apiMatchBoardJson,
  apiPlayerClubsJson,
  apiPlayerJson
}
import ccas.analysis.tables.{Club, ClubMember, MembershipRun, Player, PlayerMatchRef, RunTrigger, Tables}
import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

import TestMembershipAppSupport.*

object TestMembershipAppIntegration extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestMembershipAppIntegration")(
    suiteLookupJoinInvitations,
    suiteBuildDbState,
    suiteExternalMemberDetection,
    suiteClassifyApiMembers,
    suiteClassifyDisappeared,
    suiteReconcile
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
    val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    val summaries = List(MemberChangeSummary(pid0, Username("alice"), Chunk(NewMember(Times.t2))))
    for {
      _ <- seedDb(players = List(player))
      _ <- seedRecruitmentInvitation(clubId, pid0, Times.t1)
      result <- MembershipReport.lookupJoinInvitations(clubId, summaries)
    } yield assertTrue(
      result.size == 1,
      result(pid0) == Times.t1
    )
  }

  private def testDoesNotReturnInvitationFromDifferentClub = test("does NOT return invitation from a different club") {
    val player = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
    val summaries = List(MemberChangeSummary(pid1, Username("bob"), Chunk(NewMember(Times.t2))))
    for {
      _ <- seedDb(players = List(player))
      _ <- Club.upsert(otherClub)
      _ <- seedRecruitmentInvitation(otherClubId, pid1, Times.t1)
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
  // Suite E: buildDbState (DB)
  // ==========================================================================

  private def suiteBuildDbState = suite("buildDbState")(
    testBuildsCorrectDbStateMaps,
    testExcludesFormerMembers
  )

  private def testBuildsCorrectDbStateMaps = test("builds correct DbState maps") {
    val player0 = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
    val player1 = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t1)
    val mem0    = ClubMember(clubId, pid0, Times.t1, None, sinceApproximate = false)
    val mem1    = ClubMember(clubId, pid1, Times.t1, None, sinceApproximate = false)

    for {
      _ <- seedDb(
        players = List(player0, player1),
        members = List(mem0, mem1)
      )
      dbState <- MembershipApp.buildDbState(clubId)
    } yield assertTrue(
      dbState.membersByPlayerId.size == 2,
      dbState.membersByPlayerId.contains(pid0),
      dbState.membersByPlayerId.contains(pid1),
      dbState.membersByPlayerId(pid0).player == player0,
      dbState.membersByPlayerId(pid0).member == mem0,
      dbState.membersByUsername.size == 2,
      dbState.membersByUsername.contains(Username("alice")),
      dbState.membersByUsername.contains(Username("bob")),
      dbState.knownPlayersByUsername.contains(Username("alice")),
      dbState.knownPlayersByUsername.contains(Username("bob")),
      dbState.knownPlayersByUsername(Username("alice")) == player0,
      dbState.knownPlayersByUsername(Username("bob")) == player1
    )
  }

  private def testExcludesFormerMembers = test("excludes former members from DbState") {
    val player0   = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
    val formerMem = ClubMember(clubId, pid0, Times.t0, Some(Times.t1), sinceApproximate = false)

    for {
      _ <- seedDb(
        players = List(player0),
        members = List(formerMem)
      )
      dbState <- MembershipApp.buildDbState(clubId)
    } yield assertTrue(
      dbState.membersByPlayerId.isEmpty,
      dbState.knownPlayersByUsername.contains(Username("alice"))
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
    testSelectLatestCompletedReturnsNoneWhenNoCompletedRuns
  )

  private def testMergeResultsIncludesExternalChanges = test("mergeResults includes external changes and memberships") {
    val phaseB = PhaseBResult(Set(pid0), Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty)
    val phaseC = PhaseCResult(Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty)
    val extChange = MemberChangeSummary(pid1, Username("bob"), Chunk(JoinedClub(Times.t1)))
    val extMember = ClubMember(clubId, pid1, Times.t1, None, sinceApproximate = false)
    val result = MembershipApp.mergeResults(
      phaseB, phaseC, 10, 8, Times.t0, Times.t1,
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

  private def testSelectPlayerIdsCurrentAtReturnsCurrentMembers = test("selectPlayerIdsCurrentAt returns members current at given time") {
    val player0 = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    val player1 = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
    // alice: current member since t0
    val mem0 = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
    // bob: joined at t2, so not current at t1
    val mem1 = ClubMember(clubId, pid1, Times.t2, None, sinceApproximate = false)

    for {
      _ <- seedDb(players = List(player0, player1), members = List(mem0, mem1))
      atT1 <- ClubMember.selectPlayerIdsCurrentAt(clubId, Times.t1)
      atT3 <- ClubMember.selectPlayerIdsCurrentAt(clubId, Times.t3)
    } yield assertTrue(
      atT1 == Set(pid0),
      atT3 == Set(pid0, pid1)
    )
  }

  private def testSelectPlayerIdsCurrentAtExcludesLeftMembers = test("selectPlayerIdsCurrentAt excludes members who left before the given time") {
    val player0 = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    // alice: was member from t0 to t1
    val mem0 = ClubMember(clubId, pid0, Times.t0, Some(Times.t1), sinceApproximate = false)

    for {
      _ <- seedDb(players = List(player0), members = List(mem0))
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
      val memAlice = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
      // bob: closed but still current → excluded by status filter
      val memBob = ClubMember(clubId, pid1, Times.t0, None, sinceApproximate = false)
      // carol: active but left at t1 → excluded by window at t2
      val memCarol = ClubMember(clubId, pid2, Times.t0, Some(Times.t1), sinceApproximate = false)

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
      _ <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t2, None)
      result <- MembershipRun.selectLatestCompleted(clubId)
    } yield assertTrue(
      result.isDefined,
      result.get.startedAt == Times.t0,
      result.get.completedAt.contains(Times.t1)
    )
  }

  private def testSelectLatestCompletedReturnsNoneWhenNoCompletedRuns = test("selectLatestCompleted returns None when no completed runs exist") {
    for {
      _ <- seedDb()
      _ <- connectZIO(sql"DELETE FROM membership_run WHERE club_id = $clubId".update.run())
      _ <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t0, None)
      result <- MembershipRun.selectLatestCompleted(clubId)
    } yield assertTrue(result.isEmpty)
  }

  // ==========================================================================
  // Suite G: classifyApiMembers (DB + fake HTTP)
  // ==========================================================================

  private def suiteClassifyApiMembers = suite("classifyApiMembers")(
    testUnchangedMemberMatchingSince,
    testDifferentSinceRejoined,
    testUsernameChangeSamePlayerId,
    testNewPlayerNotInDb,
    testExistingPlayerJoinsClub,
    testUsernameChangeAndStatusChange,
    testTrustModeKnownPlayerJoins,
    testTrustModeUsernameChangeDetected,
    testSinceApproximateReplaceSince,
    testTrustUsernamesFalseBypassesLookup
  )

  private def testUnchangedMemberMatchingSince = test("unchanged member — matching since") {
    val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
    val mem    = ClubMember(clubId, pid0, Times.t1, None, sinceApproximate = false)
    val dbState = DbState(
      membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
      membersByUsername = Map(Username("alice") -> MemberState(player, mem))
    )
    val apiMap = Map(Username("alice") -> Times.t1.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
    } yield assertTrue(
      result.resolvedIds.contains(pid0),
      result.changes.isEmpty
    )
  }

  private def testDifferentSinceRejoined = test("different since → Rejoined") {
    val player = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
    val mem    = ClubMember(clubId, pid1, Times.t0, None, sinceApproximate = false)
    val dbState = DbState(
      membersByPlayerId = Map(pid1 -> MemberState(player, mem)),
      membersByUsername = Map(Username("bob") -> MemberState(player, mem))
    )
    val apiMap = Map(Username("bob") -> Times.t1.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
    } yield {
      val change = result.changes.head.changes.head
      assertTrue(
        result.resolvedIds.contains(pid1),
        result.changes.size == 1,
        change.isInstanceOf[Rejoined],
        change.timestamp == Times.t1, // API join time, not reconciliation time
        result.closedMemberships.nonEmpty,
        result.newMemberships.nonEmpty
      )
    }
  }

  private def testUsernameChangeSamePlayerId = test("username change — same player ID, different username") {
    val player = Player(pid2, Times.t0, Username("charlie-old"), Active, None, Times.t0)
    val mem    = ClubMember(clubId, pid2, Times.t0, None, sinceApproximate = false)
    val dbState = DbState(
      membersByPlayerId = Map(pid2 -> MemberState(player, mem)),
      membersByUsername = Map(Username("charlie-old") -> MemberState(player, mem))
    )
    val apiMap    = Map(Username("charlie-new") -> Times.t0.getEpochSecond)
    val responses = Map("charlie-new" -> apiPlayerJson(102, "charlie-new"))

    for {
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
    } yield assertTrue(
      result.resolvedIds.contains(pid2),
      result.changes.size == 1,
      result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
      result.updatedPlayers.nonEmpty
    )
  }

  private def testNewPlayerNotInDb = test("new player — not in DB") {
    val dbState   = DbState(Map.empty, Map.empty)
    val apiMap    = Map(Username("diana") -> Times.t0.getEpochSecond)
    val responses = Map("diana" -> apiPlayerJson(103, "diana"))

    for {
      _      <- seedDb()
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
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
    val dbState   = DbState(Map.empty, Map.empty)
    val apiMap    = Map(Username("eve") -> Times.t1.getEpochSecond)
    val responses = Map("eve" -> apiPlayerJson(104, "eve"))

    for {
      _      <- seedDb(players = List(player4))
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
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

  private def testUsernameChangeAndStatusChange = test("username change + status change") {
    val player = Player(pid5, Times.t0, Username("frank-old"), Active, None, Times.t0)
    val mem    = ClubMember(clubId, pid5, Times.t0, None, sinceApproximate = false)
    val dbState = DbState(
      membersByPlayerId = Map(pid5 -> MemberState(player, mem)),
      membersByUsername = Map(Username("frank-old") -> MemberState(player, mem))
    )
    val apiMap    = Map(Username("frank-new") -> Times.t0.getEpochSecond)
    val responses = Map("frank-new" -> apiPlayerJson(105, "frank-new", status = "closed"))

    for {
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
    } yield assertTrue(
      result.resolvedIds.contains(pid5),
      result.changes.size == 1,
      result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
      result.changes.head.changes.exists(_.isInstanceOf[StatusChange])
    )
  }

  private def testTrustModeKnownPlayerJoins = test("trust-mode: known player joins club without API call") {
    val player = Player(pid3, Times.t0, Username("diana"), Active, None, Times.t0)
    val dbState = DbState(
      membersByPlayerId = Map.empty,
      membersByUsername = Map.empty,
      knownPlayersByUsername = Map(Username("diana") -> player)
    )
    val apiMap = Map(Username("diana") -> Times.t1.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
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

  private def testTrustModeUsernameChangeDetected = test("trust-mode: username change detected without API call") {
    val oldPlayer = Player(pid2, Times.t0, Username("charlie-old"), Active, None, Times.t0)
    val mem       = ClubMember(clubId, pid2, Times.t0, None, sinceApproximate = false)
    val newPlayer = Player(pid2, Times.t0, Username("charlie-new"), Active, None, Times.t1)
    val dbState = DbState(
      membersByPlayerId = Map(pid2 -> MemberState(oldPlayer, mem)),
      membersByUsername = Map(Username("charlie-old") -> MemberState(oldPlayer, mem)),
      knownPlayersByUsername = Map(Username("charlie-new") -> newPlayer)
    )
    val apiMap = Map(Username("charlie-new") -> Times.t0.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
    } yield assertTrue(
      result.resolvedIds.contains(pid2),
      result.changes.size == 1,
      result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
      result.updatedPlayers.nonEmpty
    )
  }

  private def testSinceApproximateReplaceSince = test("sinceApproximate member → replaceSince, not Rejoined") {
    val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    val mem    = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = true)
    val dbState = DbState(
      membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
      membersByUsername = Map(Username("alice") -> MemberState(player, mem))
    )
    val apiMap = Map(Username("alice") -> Times.t1.getEpochSecond)

    for {
      _ <- seedDb(
        players = List(player),
        members = List(mem)
      )
      client  <- fakeChessComClient(Map.empty)
      result  <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      members <- ClubMember.selectClub(clubId)
    } yield assertTrue(
      result.resolvedIds.contains(pid0),
      result.changes.isEmpty,
      result.newMemberships.isEmpty,
      result.closedMemberships.isEmpty,
      members.size == 1,
      members.head.since == Times.t1,
      !members.head.sinceApproximate
    )
  }

  private def testTrustUsernamesFalseBypassesLookup = test("trustUsernames=false bypasses known player lookup") {
    val player = Player(pid3, Times.t0, Username("diana"), Active, None, Times.t0)
    val dbState = DbState(
      membersByPlayerId = Map.empty,
      membersByUsername = Map.empty,
      knownPlayersByUsername = Map(Username("diana") -> player)
    )
    val apiMap = Map(Username("diana") -> Times.t0.getEpochSecond)

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyApiMembers(
        client,
        clubId,
        apiMap,
        dbState,
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
    testClosedPlayerAccountClosed,
    testClosedPlayerStillInClub,
    testAlreadyClosedPlayerSilent,
    testMatchRefFallbackRenamedStillInClub,
    testMatchRefFallbackRenamedLeftClub,
    testApi404Unresolvable,
    testDifferentPlayerIdUnresolvable,
    testAllResolvedEmptyResults
  )

  private def testActivePlayerLeftClub = test("active player left club → LeftClub with now timestamp") {
    val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    val mem    = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
    val dbState = DbState(
      membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
      membersByUsername = Map(Username("alice") -> MemberState(player, mem))
    )
    val responses = Map("alice" -> apiPlayerJson(100, "alice"))

    for {
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyDisappeared(
        client,
        dbState,
        Set.empty,
        Map.empty,
        ClubSlug("test-club"),
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

  private def testClosedPlayerAccountClosed =
    test("closed player not in own clubs list → AccountClosed, membership closed at lastOnline") {
      val player = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid1, Times.t0, None, sinceApproximate = false)
      val dbState = DbState(
        membersByPlayerId = Map(pid1 -> MemberState(player, mem)),
        membersByUsername = Map(Username("bob") -> MemberState(player, mem))
      )
      val responses =
        Map("bob" -> apiPlayerJson(101, "bob", status = "closed", lastOnline = Some(Times.t1.getEpochSecond)))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyDisappeared(
          client,
          dbState,
          Set.empty,
          Map.empty,
          ClubSlug("test-club"),
          Times.t2
        )
      } yield {
        val changes = result.changes.head.changes
        assertTrue(
          result.changes.size == 1,
          changes.size == 1,
          changes.head.isInstanceOf[AccountClosed],
          changes.head.timestamp == Times.t1, // lastOnline, not reconciliation time
          !changes.exists(_.isInstanceOf[StatusChange]),
          result.updatedPlayers.nonEmpty,
          result.closedMemberships.nonEmpty,
          result.closedMemberships.head.until.contains(Times.t1) // until matches lastOnline
        )
      }
    }

  private def testClosedPlayerStillInClub =
    test("closed player still in own clubs list → AccountClosed only, membership not closed") {
      val player = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid1, Times.t0, None, sinceApproximate = false)
      val dbState = DbState(
        membersByPlayerId = Map(pid1 -> MemberState(player, mem)),
        membersByUsername = Map(Username("bob") -> MemberState(player, mem))
      )
      val responses =
        Map("bob" -> apiPlayerJson(101, "bob", status = "closed", lastOnline = Some(Times.t1.getEpochSecond)))
      val clubsResponses = Map("bob" -> apiPlayerClubsJson(List("test-club")))

      for {
        client <- fakeChessComClient(responses, clubsResponses = clubsResponses)
        result <- MembershipClassify.classifyDisappeared(
          client,
          dbState,
          Set.empty,
          Map.empty,
          ClubSlug("test-club"),
          Times.t2
        )
      } yield {
        val changes = result.changes.head.changes
        assertTrue(
          result.changes.size == 1,
          changes.size == 1,
          changes.head.isInstanceOf[AccountClosed],
          changes.head.timestamp == Times.t1,
          !changes.exists(_.isInstanceOf[StatusChange]),
          result.updatedPlayers.nonEmpty,
          result.updatedPlayers.head.status == Closed,
          result.archivedSnapshots.nonEmpty,
          result.closedMemberships.isEmpty
        )
      }
    }

  private def testAlreadyClosedPlayerSilent =
    test("already-closed player, still in own clubs list → no changes emitted") {
      val player = Player(pid1, Times.t0, Username("bob"), Closed, None, Times.t0)
      val mem    = ClubMember(clubId, pid1, Times.t0, None, sinceApproximate = false)
      val dbState = DbState(
        membersByPlayerId = Map(pid1 -> MemberState(player, mem)),
        membersByUsername = Map(Username("bob") -> MemberState(player, mem))
      )
      val responses =
        Map("bob" -> apiPlayerJson(101, "bob", status = "closed", lastOnline = Some(Times.t1.getEpochSecond)))
      val clubsResponses = Map("bob" -> apiPlayerClubsJson(List("test-club")))

      for {
        client <- fakeChessComClient(responses, clubsResponses = clubsResponses)
        result <- MembershipClassify.classifyDisappeared(
          client,
          dbState,
          Set.empty,
          Map.empty,
          ClubSlug("test-club"),
          Times.t2
        )
      } yield assertTrue(
        result.changes.isEmpty,
        result.updatedPlayers.isEmpty,
        result.archivedSnapshots.isEmpty,
        result.closedMemberships.isEmpty
      )
    }

  private val matchRefId = ClubMatchId(999000L)

  // Runs classifyDisappeared for a player whose old username 404s and resolves via match-ref to "ed-new".
  // Caller controls apiMap to simulate "still in club under new name" vs "actually left".
  private def runMatchRefFallbackRenamed(apiMap: Map[Username, Long]) = {
    val player = Player(pid5, Times.t0, Username("ed-old"), Active, None, Times.t0)
    val mem    = ClubMember(clubId, pid5, Times.t0, None, sinceApproximate = false)
    val ref    = PlayerMatchRef(pid5, matchRefId, isLive = false, isTeam1 = true, boardIdx = 1)
    val dbState = DbState(
      membersByPlayerId = Map(pid5 -> MemberState(player, mem)),
      membersByUsername = Map(Username("ed-old") -> MemberState(player, mem))
    )
    val responses = Map("ed-new" -> apiPlayerJson(105, "ed-new"))
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
        responses,
        failures = Set("ed-old"),
        matchResponses = matchResponses,
        boardResponses = boardResponses
      )
      result <- MembershipClassify.classifyDisappeared(
        client, dbState, Set.empty, apiMap, ClubSlug("test-club"), Times.t2
      )
    } yield result
  }

  private def testMatchRefFallbackRenamedStillInClub =
    test("matchRefFallback: renamed player still in club → no LeftClub, membership not closed") {
      runMatchRefFallbackRenamed(Map(Username("ed-new") -> Times.t0.getEpochSecond)).map { result =>
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
      runMatchRefFallbackRenamed(Map.empty).map { result =>
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

  private def testApi404Unresolvable = test("API 404 → Unresolvable") {
    val player = Player(pid2, Times.t0, Username("charlie"), Active, None, Times.t0)
    val mem    = ClubMember(clubId, pid2, Times.t0, None, sinceApproximate = false)
    val dbState = DbState(
      membersByPlayerId = Map(pid2 -> MemberState(player, mem)),
      membersByUsername = Map(Username("charlie") -> MemberState(player, mem))
    )

    for {
      client <- fakeChessComClient(Map.empty, failures = Set("charlie"))
      result <- MembershipClassify.classifyDisappeared(
        client,
        dbState,
        Set.empty,
        Map.empty,
        ClubSlug("test-club"),
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
    val mem    = ClubMember(clubId, pid3, Times.t0, None, sinceApproximate = false)
    val dbState = DbState(
      membersByPlayerId = Map(pid3 -> MemberState(player, mem)),
      membersByUsername = Map(Username("diana") -> MemberState(player, mem))
    )
    val responses = Map("diana" -> apiPlayerJson(999, "diana"))

    for {
      client <- fakeChessComClient(responses)
      result <- MembershipClassify.classifyDisappeared(
        client,
        dbState,
        Set.empty,
        Map.empty,
        ClubSlug("test-club"),
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
    val mem    = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
    val dbState = DbState(
      membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
      membersByUsername = Map(Username("alice") -> MemberState(player, mem))
    )

    for {
      client <- fakeChessComClient(Map.empty)
      result <- MembershipClassify.classifyDisappeared(
        client,
        dbState,
        Set(pid0),
        Map.empty,
        ClubSlug("test-club"),
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
    testDeltaReflectsNewJoinAcrossRuns
  )

  private def testDeltaReflectsNewJoinAcrossRuns =
    test("previousMemberCount ignores rows this run inserts (regression)") {
      // Regression: previousMemberCount used to be sampled after this run completed itself,
      // so selectLatestCompleted returned THIS run and the delta always collapsed to 0.
      val priorCompletedAt = Times.t1
      val bobJoined        = Times.t1.minusSeconds(600) // before the prior run completed
      val alice            = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val aliceMembership  = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "club" / string("slug") / "members" -> handler {
          (_: String, _: Request) =>
            Response.json(
              apiClubMembersJson(
                List(
                  ("alice", Times.t0.getEpochSecond),
                  ("bob", bobJoined.getEpochSecond)
                )
              )
            )
        },
        Method.GET / "pub" / "club" / string("slug") -> handler { (slug: String, _: Request) =>
          Response.json(apiClubJson(ClubId.unwrap(clubId), slug))
        },
        Method.GET / "pub" / "player" / string("username") -> handler {
          (username: String, _: Request) =>
            username match {
              case "alice" => Response.json(apiPlayerJson(PlayerId.unwrap(pid0), "alice"))
              case "bob" =>
                Response.json(
                  apiPlayerJson(PlayerId.unwrap(pid1), "bob", joined = bobJoined.getEpochSecond)
                )
              case _ => Response.json("""{"code": 0, "message": "Resource \"\" not found."}""").copy(status = Status.NotFound)
            }
        }
      )
      for {
        _          <- connectZIO(sql"DELETE FROM membership_run WHERE club_id = $clubId".update.run())
        _          <- seedDb(players = List(alice), members = List(aliceMembership))
        priorRunId <- MembershipRun.insert(clubId, RunTrigger.Cli, Times.t0, None)
        _          <- MembershipRun.complete(priorRunId, priorCompletedAt)
        client     <- TestChessComClientSupport.fakeClient(routes)
        result <- MembershipApp.reconcile(ClubSlug("test-club"))
                    .provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
      } yield assertTrue(
        result.newMemberships.size == 1,
        result.previousMemberCount == 1,
        result.currentMemberCount == 2
      )
    }
}
