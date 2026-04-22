package ccas.analysis.apps.ref

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.Scope
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiDailyMatchJson
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubMatchId, TournamentSlug}
import ccas.utils.sql.FreshSchemaLayer

import TestRefAppSupport.*

object TestRefAppSkipAndUpgrade extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRefAppSkipAndUpgrade")(
    suiteSkipTracking,
    suiteForceSkipped,
    suiteTournamentSorting,
    suiteUpgrade
  ).provideShared(
    FreshSchemaLayer("test_ref_app_skip_upgrade", onInit = Tables.ensureTables),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: skip tracking
  // ==========================================================================

  private def suiteSkipTracking = suite("skip tracking")(
    testWritesNoDataSkipForPlayerWithNoMatchesAndNoTournaments,
    testWritesNoDataSkipForClubWithNoFinishedMatches,
    testWritesApiErrorSkipForPlayerWithApiFailure,
    testWritesResolutionFailedSkipWhenPlayerHasMatchesButNotInRoster,
    testSkippedPlayerIsExcludedFromSubsequentRun,
    testExpiredSkipAllowsRetryAndSkipRowDeletedOnResolution
  )

  private def testWritesNoDataSkipForPlayerWithNoMatchesAndNoTournaments = test("writes NoData skip for player with no matches and no tournaments") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> emptyPlayerMatchesJson,
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson
        )
      )
      _     <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      skip0 <- PlayerRefSkip.selectId(pid0)
      skip1 <- PlayerRefSkip.selectId(pid1)
      skip2 <- PlayerRefSkip.selectId(pid2)
    } yield assertTrue(
      skip0.isDefined,
      skip0.get.reason == RefSkipReason.NoData,
      skip1.isDefined,
      skip1.get.reason == RefSkipReason.NoData,
      skip2.isDefined,
      skip2.get.reason == RefSkipReason.NoData
    )
  }

  private def testWritesNoDataSkipForClubWithNoFinishedMatches = test("writes NoData skip for club with no finished matches") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> emptyPlayerMatchesJson,
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson
        )
      )
      _     <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      skip0 <- ClubRefSkip.selectId(clubId0)
      skip1 <- ClubRefSkip.selectId(clubId1)
    } yield assertTrue(
      skip0.isDefined,
      skip0.get.reason == RefSkipReason.NoData,
      skip1.isDefined,
      skip1.get.reason == RefSkipReason.NoData
    )
  }

  private def testWritesApiErrorSkipForPlayerWithApiFailure = test("writes ApiError skip for player with API failure") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        responses = Map(
          s"player/alice/matches"    -> emptyPlayerMatchesJson,
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson
        ),
        failures = Set("charlie")
      )
      _    <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      skip <- PlayerRefSkip.selectId(pid2)
    } yield assertTrue(
      skip.isDefined,
      skip.get.reason == RefSkipReason.ApiError
    )
  }

  private def testWritesResolutionFailedSkipWhenPlayerHasMatchesButNotInRoster = test("writes ResolutionFailed skip when player has matches but is not in roster") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "club-x",
      "club-y",
      team1Players = List(("stranger1", 1)),
      team2Players = List(("stranger2", 2))
    )
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      _    <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      skip <- PlayerRefSkip.selectId(pid0)
    } yield assertTrue(
      skip.isDefined,
      skip.get.reason == RefSkipReason.ResolutionFailed
    )
  }

  private def testSkippedPlayerIsExcludedFromSubsequentRun = test("skipped player is excluded from subsequent run") {
    for {
      _ <- seedDb
      // Run 1: all fail -> skip rows written
      client1 <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> emptyPlayerMatchesJson,
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson
        )
      )
      _             <- runPopulate(client1, forceSkipped = false, upgradeRefs = false)
      skipAfterRun1 <- PlayerRefSkip.selectId(pid0)
      // Run 2: provide data that WOULD resolve alice -- but she should be skipped
      matchJson = apiDailyMatchJson(
        matchId1,
        "our-club",
        "other-club",
        team1Players = List(("alice", 3)),
        team2Players = List(("opponent1", 1))
      )
      client2 <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      _             <- runPopulate(client2, forceSkipped = false, upgradeRefs = false)
      ref           <- PlayerMatchRef.selectId(pid0)
      skipAfterRun2 <- PlayerRefSkip.selectId(pid0)
    } yield assertTrue(
      skipAfterRun1.isDefined,
      ref.isEmpty, // alice was not resolved -- she was skipped
      skipAfterRun2.isDefined,
      skipAfterRun2.get.lastAttempted == skipAfterRun1.get.lastAttempted // not re-attempted
    )
  }

  private def testExpiredSkipAllowsRetryAndSkipRowDeletedOnResolution = test("expired skip allows retry and skip row is deleted on resolution") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "our-club",
      "other-club",
      team1Players = List(("alice", 3)),
      team2Players = List(("opponent1", 1))
    )
    for {
      _ <- seedDb
      // Seed an expired skip for alice (last attempted 15 days ago, NoData window is 14 days)
      expiredTime = Instant.now().minus(15, ChronoUnit.DAYS)
      _ <- PlayerRefSkip.upsert(PlayerRefSkip(pid0, RefSkipReason.NoData, None, expiredTime))
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      _    <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref  <- PlayerMatchRef.selectId(pid0)
      skip <- PlayerRefSkip.selectId(pid0)
    } yield assertTrue(
      ref.isDefined, // alice was resolved
      ref.get.boardIdx == 3,
      skip.isEmpty // skip row was cleaned up
    )
  }

  // ==========================================================================
  // Suite: forceSkipped flag
  // ==========================================================================

  private def suiteForceSkipped = suite("forceSkipped")(
    testForceSkippedReProcessesPlayersWithActiveSkipRecords,
    testForceSkippedReProcessesClubsWithActiveSkipRecords
  )

  private def testForceSkippedReProcessesPlayersWithActiveSkipRecords = test("forceSkipped re-processes players with active skip records") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "our-club",
      "other-club",
      team1Players = List(("alice", 3)),
      team2Players = List(("opponent1", 1))
    )
    for {
      _ <- seedDb
      // Seed a fresh skip for alice (within retry window)
      _ <- PlayerRefSkip.upsert(PlayerRefSkip(pid0, RefSkipReason.NoData, None, Instant.now()))
      // Normal run: alice should be skipped
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      _         <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      refBefore <- PlayerMatchRef.selectId(pid0)
      // Force run: alice should be re-processed and resolved
      _         <- runPopulate(client, forceSkipped = true, upgradeRefs = false)
      refAfter  <- PlayerMatchRef.selectId(pid0)
      skipAfter <- PlayerRefSkip.selectId(pid0)
    } yield assertTrue(
      refBefore.isEmpty,  // not resolved on normal run
      refAfter.isDefined, // resolved on forced run
      refAfter.get.boardIdx == 3,
      skipAfter.isEmpty // skip row cleaned up
    )
  }

  private def testForceSkippedReProcessesClubsWithActiveSkipRecords = test("forceSkipped re-processes clubs with active skip records") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "our-club",
      "other-club",
      team1Players = List(("player1", 1)),
      team2Players = List(("player2", 2))
    )
    for {
      _ <- seedDb
      // Seed a fresh skip for our-club
      _ <- ClubRefSkip.upsert(ClubRefSkip(clubId0, RefSkipReason.NoData, None, Instant.now()))
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> emptyPlayerMatchesJson,
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> apiClubMatchesJson(List(matchId1)),
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      // Normal run: club should be skipped
      _         <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      refBefore <- ClubMatchRef.selectId(clubId0)
      // Force run: club should be re-processed and resolved
      _         <- runPopulate(client, forceSkipped = true, upgradeRefs = false)
      refAfter  <- ClubMatchRef.selectId(clubId0)
      skipAfter <- ClubRefSkip.selectId(clubId0)
    } yield assertTrue(
      refBefore.isEmpty,
      refAfter.isDefined,
      refAfter.get.matchId == ClubMatchId.wrap(matchId1),
      skipAfter.isEmpty
    )
  }

  // ==========================================================================
  // Suite: tournament sorting by size
  // ==========================================================================

  private def suiteTournamentSorting = suite("tournament sorting")(
    testPrefersSmallerTournamentWhenMultipleAreAvailable
  )

  private def testPrefersSmallerTournamentWhenMultipleAreAvailable = test("prefers smaller tournament when multiple are available") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"   -> emptyPlayerMatchesJson,
          s"player/bob/matches"     -> emptyPlayerMatchesJson,
          s"player/charlie/matches" -> emptyPlayerMatchesJson,
          // big-tourney listed first in API response but has more players
          s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List(("big-tourney", 100), ("small-tourney", 4))),
          s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
          s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
          s"tournament/big-tourney/1"   -> apiTournamentRoundJson(List("alice", "other1", "other2")),
          s"tournament/small-tourney/1" -> apiTournamentRoundJson(List("other3", "alice"))
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      ref.isDefined,
      ref.get.tournamentSlug == TournamentSlug("small-tourney"),
      ref.get.playerIdx == 1
    )
  }

  // ==========================================================================
  // Suite: tournament -> match upgrade
  // ==========================================================================

  private def suiteUpgrade = suite("tournament to match upgrade")(
    testUpgradesTournamentRefToMatchRef,
    testLeavesTournamentRefIntactWhenMatchResolutionFails,
    testUpgradesTournamentRefToSmallerTournament,
    testLeavesTournamentRefUnchangedWhenAlreadySmallest,
    testSkipsFailedSmallerTournamentAndKeepsCurrentRef,
    testUpgradePhaseDoesNotRunWhenUpgradeRefsIsFalse
  )

  private def testUpgradesTournamentRefToMatchRef = test("upgrades tournament ref to match ref when match data is available") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "our-club",
      "other-club",
      team1Players = List(("alice", 3)),
      team2Players = List(("opponent1", 1))
    )
    for {
      _ <- seedDb
      // Pre-seed a tournament ref for alice
      _ <- PlayerTournamentRef.insert(PlayerTournamentRef(pid0, TournamentSlug("tourney-1"), 1))
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      _        <- runPopulate(client, forceSkipped = false, upgradeRefs = true)
      matchRef <- PlayerMatchRef.selectId(pid0)
      tournRef <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      matchRef.isDefined,
      matchRef.get.matchId == ClubMatchId.wrap(matchId1),
      matchRef.get.boardIdx == 3,
      tournRef.isEmpty // tournament ref was deleted after upgrade
    )
  }

  private def testLeavesTournamentRefIntactWhenMatchResolutionFails = test("leaves tournament ref intact when match resolution fails") {
    for {
      _ <- seedDb
      // Pre-seed a tournament ref for alice
      _ <- PlayerTournamentRef.insert(PlayerTournamentRef(pid0, TournamentSlug("tourney-1"), 1))
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> emptyPlayerMatchesJson,
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson
        )
      )
      _        <- runPopulate(client, forceSkipped = false, upgradeRefs = true)
      matchRef <- PlayerMatchRef.selectId(pid0)
      tournRef <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      matchRef.isEmpty,
      tournRef.isDefined,
      tournRef.get.tournamentSlug == TournamentSlug("tourney-1")
    )
  }

  private def testUpgradesTournamentRefToSmallerTournament = test("upgrades tournament ref to smaller tournament when match upgrade fails") {
    for {
      _ <- seedDb
      _ <- PlayerTournamentRef.insert(PlayerTournamentRef(pid0, TournamentSlug("tourney-big"), 0))
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"       -> emptyPlayerMatchesJson,
          s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List(("tourney-small", 4), ("tourney-big", 50))),
          s"player/bob/matches"         -> emptyPlayerMatchesJson,
          s"player/charlie/matches"     -> emptyPlayerMatchesJson,
          s"club/our-club/matches"      -> emptyClubMatchesJson,
          s"club/other-club/matches"    -> emptyClubMatchesJson,
          s"tournament/tourney-small/1" -> apiTournamentRoundJson(List("someone", "alice", "other"))
        )
      )
      _        <- runPopulate(client, forceSkipped = false, upgradeRefs = true)
      matchRef <- PlayerMatchRef.selectId(pid0)
      tournRef <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      matchRef.isEmpty,
      tournRef.isDefined,
      tournRef.get.tournamentSlug == TournamentSlug("tourney-small"),
      tournRef.get.playerIdx == 1 // index of "alice" in round players
    )
  }

  private def testLeavesTournamentRefUnchangedWhenAlreadySmallest = test("leaves tournament ref unchanged when already the smallest") {
    for {
      _ <- seedDb
      _ <- PlayerTournamentRef.insert(PlayerTournamentRef(pid0, TournamentSlug("tourney-small"), 1))
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"       -> emptyPlayerMatchesJson,
          s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List(("tourney-small", 4), ("tourney-big", 50))),
          s"player/bob/matches"         -> emptyPlayerMatchesJson,
          s"player/charlie/matches"     -> emptyPlayerMatchesJson,
          s"club/our-club/matches"      -> emptyClubMatchesJson,
          s"club/other-club/matches"    -> emptyClubMatchesJson,
          s"tournament/tourney-small/1" -> apiTournamentRoundJson(List("someone", "alice"))
        )
      )
      _        <- runPopulate(client, forceSkipped = false, upgradeRefs = true)
      tournRef <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      tournRef.isDefined,
      tournRef.get.tournamentSlug == TournamentSlug("tourney-small"),
      tournRef.get.playerIdx == 1 // unchanged
    )
  }

  private def testSkipsFailedSmallerTournamentAndKeepsCurrentRef = test("skips failed smaller tournament and keeps current ref") {
    for {
      _ <- seedDb
      _ <- PlayerTournamentRef.insert(PlayerTournamentRef(pid0, TournamentSlug("tourney-medium"), 2))
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"        -> emptyPlayerMatchesJson,
          s"player/alice/tournaments"    -> apiPlayerTournamentsJson(List(("tourney-tiny", 2), ("tourney-medium", 20))),
          s"player/bob/matches"          -> emptyPlayerMatchesJson,
          s"player/charlie/matches"      -> emptyPlayerMatchesJson,
          s"club/our-club/matches"       -> emptyClubMatchesJson,
          s"club/other-club/matches"     -> emptyClubMatchesJson,
          s"tournament/tourney-medium/1" -> apiTournamentRoundJson(List("x", "y", "alice"))
          // tourney-tiny/1 not provided -> 404
        )
      )
      _        <- runPopulate(client, forceSkipped = false, upgradeRefs = true)
      tournRef <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      tournRef.isDefined,
      tournRef.get.tournamentSlug == TournamentSlug("tourney-medium"),
      tournRef.get.playerIdx == 2 // unchanged
    )
  }

  private def testUpgradePhaseDoesNotRunWhenUpgradeRefsIsFalse = test("upgrade phase does not run when upgradeRefs is false") {
    for {
      _ <- seedDb
      // Pre-seed a tournament ref for alice
      _ <- PlayerTournamentRef.insert(PlayerTournamentRef(pid0, TournamentSlug("tourney-1"), 1))
      matchJson = apiDailyMatchJson(
        matchId1,
        "our-club",
        "other-club",
        team1Players = List(("alice", 3)),
        team2Players = List(("opponent1", 1))
      )
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      _        <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      matchRef <- PlayerMatchRef.selectId(pid0)
      tournRef <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      matchRef.isEmpty,  // no upgrade attempted
      tournRef.isDefined // tournament ref unchanged
    )
  }
}
