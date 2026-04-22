package ccas.analysis.apps.ref

import zio.Scope
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiDailyMatchJson
import ccas.analysis.tables.{ClubMatchRef, PlayerMatchRef, PlayerTournamentRef, Tables}
import ccas.api.misc.subtypes.{ClubMatchId, TournamentSlug}
import ccas.utils.sql.FreshSchemaLayer

import TestRefAppSupport.*

object TestRefAppIteration extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRefAppIteration")(
    suiteIteration,
    suiteFullPopulate
  ).provideShared(
    FreshSchemaLayer("test_ref_app_iteration", onInit = Tables.ensureTables),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: iteration and failed-URL cache
  // ==========================================================================

  private val matchId3 = 9003L

  private def suiteIteration = suite("iteration and failed-URL cache")(
    testFallsBackToSecondMatchWhenFirstMatch404s,
    testFallsBackToTournamentWhenAllMatchesFail,
    testFallsBackToSecondTournamentWhenFirstTournamentRound404s,
    testFailedTournamentUrlIsNotRetriedForAnotherPlayer,
    testClubResolutionIteratesPastFailedMatch
  )

  private def testFallsBackToSecondMatchWhenFirstMatch404s = test("falls back to second match when first match 404s") {
    val matchJson2 = apiDailyMatchJson(
      matchId2,
      "our-club",
      "other-club",
      team1Players = List(("alice", 5)),
      team2Players = List(("opponent1", 1))
    )
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          // alice has two matches; matchId1 will 404 (not in responses), matchId2 succeeds
          s"player/alice/matches"   -> apiPlayerMatchesJson(List((matchId1, Some(3)), (matchId2, Some(5)))),
          s"player/bob/matches"     -> emptyPlayerMatchesJson,
          s"player/charlie/matches" -> emptyPlayerMatchesJson,
          s"match/$matchId2"        -> matchJson2
          // match/$matchId1 not present -> 404
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerMatchRef.selectId(pid0)
    } yield assertTrue(
      ref.isDefined,
      ref.get.matchId == ClubMatchId.wrap(matchId2),
      ref.get.boardIdx == 5
    )
  }

  private def testFallsBackToTournamentWhenAllMatchesFail = test("falls back to tournament when all matches fail") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          // alice has a match that will 404, then falls back to tournament
          s"player/alice/matches"       -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"         -> emptyPlayerMatchesJson,
          s"player/charlie/matches"     -> emptyPlayerMatchesJson,
          s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List(("tourney-1", 5))),
          s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
          s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
          s"tournament/tourney-1/1"     -> apiTournamentRoundJson(List("alice", "other-player"))
          // match/$matchId1 not present -> 404
        )
      )
      _        <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      matchRef <- PlayerMatchRef.selectId(pid0)
      tournRef <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      matchRef.isEmpty,
      tournRef.isDefined,
      tournRef.get.tournamentSlug == TournamentSlug("tourney-1"),
      tournRef.get.playerIdx == 0
    )
  }

  private def testFallsBackToSecondTournamentWhenFirstTournamentRound404s = test("falls back to second tournament when first tournament round 404s") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"       -> emptyPlayerMatchesJson,
          s"player/bob/matches"         -> emptyPlayerMatchesJson,
          s"player/charlie/matches"     -> emptyPlayerMatchesJson,
          s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List(("bad-tourney", 5), ("good-tourney", 5))),
          s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
          s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
          // bad-tourney/1 not present -> 404
          s"tournament/good-tourney/1" -> apiTournamentRoundJson(List("alice", "someone"))
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      ref.isDefined,
      ref.get.tournamentSlug == TournamentSlug("good-tourney"),
      ref.get.playerIdx == 0
    )
  }

  private def testFailedTournamentUrlIsNotRetriedForAnotherPlayer = test("failed tournament URL is not retried for another player") {
    // Both alice and bob share bad-tourney (which 404s) and good-tourney
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"       -> emptyPlayerMatchesJson,
          s"player/bob/matches"         -> emptyPlayerMatchesJson,
          s"player/charlie/matches"     -> emptyPlayerMatchesJson,
          s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List(("bad-tourney", 5), ("good-tourney", 5))),
          s"player/bob/tournaments"     -> apiPlayerTournamentsJson(List(("bad-tourney", 5), ("good-tourney", 5))),
          s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
          // bad-tourney/1 not present -> 404 (should only be tried once across both players)
          s"tournament/good-tourney/1" -> apiTournamentRoundJson(List("alice", "bob", "someone"))
        )
      )
      _        <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      aliceRef <- PlayerTournamentRef.selectId(pid0)
      bobRef   <- PlayerTournamentRef.selectId(pid1)
    } yield assertTrue(
      aliceRef.isDefined,
      aliceRef.get.tournamentSlug == TournamentSlug("good-tourney"),
      bobRef.isDefined,
      bobRef.get.tournamentSlug == TournamentSlug("good-tourney")
    )
  }

  private def testClubResolutionIteratesPastFailedMatch = test("club resolution iterates past failed match") {
    val matchJson3 = apiDailyMatchJson(
      matchId3,
      "our-club",
      "other-club",
      team1Players = List(("player1", 1)),
      team2Players = List(("player2", 2))
    )
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> emptyPlayerMatchesJson,
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> apiClubMatchesJson(List(matchId1, matchId3)),
          s"club/other-club/matches" -> emptyClubMatchesJson,
          // match/$matchId1 not present -> 404
          s"match/$matchId3" -> matchJson3
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- ClubMatchRef.selectId(clubId0)
    } yield assertTrue(
      ref.isDefined,
      ref.get.matchId == ClubMatchId.wrap(matchId3),
      ref.get.isTeam1
    )
  }

  // ==========================================================================
  // Suite: full populate
  // ==========================================================================

  private def suiteFullPopulate = suite("full populate")(
    testResolvesBothPlayersAndClubsInOneRun,
    testAlreadyResolvedEntitiesAreNotReProcessed
  )

  private def testResolvesBothPlayersAndClubsInOneRun = test("resolves both players and clubs in one run") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "our-club",
      "other-club",
      team1Players = List(("alice", 3)),
      team2Players = List(("opponent1", 1))
    )
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"    -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/our-club/matches"   -> apiClubMatchesJson(List(matchId1)),
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      _         <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      playerRef <- PlayerMatchRef.selectId(pid0)
      clubRef   <- ClubMatchRef.selectId(clubId0)
    } yield assertTrue(
      playerRef.isDefined,
      playerRef.get.matchId == ClubMatchId.wrap(matchId1),
      clubRef.isDefined,
      clubRef.get.matchId == ClubMatchId.wrap(matchId1)
    )
  }

  private def testAlreadyResolvedEntitiesAreNotReProcessed = test("already-resolved entities are not re-processed") {
    for {
      _ <- seedDb
      // Pre-seed match refs
      _ <- PlayerMatchRef.insert(PlayerMatchRef(pid0, ClubMatchId.wrap(matchId1), isLive = false, true, 3))
      _ <- ClubMatchRef.insert(ClubMatchRef(clubId0, ClubMatchId.wrap(matchId1), isLive = false, true))
      // Provide no API responses -- if populate tries to fetch, it would get empty/404
      client <- fakeChessComClient(
        Map(
          s"player/bob/matches"      -> emptyPlayerMatchesJson,
          s"player/charlie/matches"  -> emptyPlayerMatchesJson,
          s"club/other-club/matches" -> emptyClubMatchesJson
        )
      )
      _         <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      playerRef <- PlayerMatchRef.selectId(pid0)
      clubRef   <- ClubMatchRef.selectId(clubId0)
    } yield assertTrue(
      playerRef.isDefined,
      playerRef.get.matchId == ClubMatchId.wrap(matchId1),
      playerRef.get.isTeam1,
      playerRef.get.boardIdx == 3,
      clubRef.isDefined,
      clubRef.get.matchId == ClubMatchId.wrap(matchId1),
      clubRef.get.isTeam1
    )
  }
}
