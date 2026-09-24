package ccas.analysis.apps.ref

import zio.Scope
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiDailyMatchJson
import ccas.analysis.apps.UsernameRenameResolver
import ccas.analysis.tables.{
  Club,
  ClubMatchRef,
  ClubRefSkip,
  Player,
  PlayerMatchRef,
  PlayerRefSkip,
  PlayerTournamentRef,
  Tables
}
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubMatchId, TournamentSlug, Username}
import ccas.utils.sql.FreshSchemaLayer

import TestRefAppSupport.*

object TestRefAppResolution extends ZIOSpecDefault {

  private val t1 = t0.plusSeconds(86400)

  override def spec: Spec[Any, Throwable] = suite("TestRefAppResolution")(
    suitePlayerResolution,
    suiteClubResolution,
    suiteTournamentResolution
  ).provideShared(
    FreshSchemaLayer("test_ref_app_resolution", onInit = Tables.ensureTables),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: player resolution
  // ==========================================================================

  // #254: a player holding no name has nothing to be fetched under, so it is not selected at all — not fetched, and
  // not skipped either, since a skip would record a failure it never had.
  private def testLeavesPlayerHoldingNoNameAlone = test("leaves a player holding no name alone") {
    for {
      _ <- seedDb
      _ <- Player.updateCurrentState(Player(pid0, t0, UsernameRenameResolver.stalePlaceholder(pid0), Active, None, t1))
      _ <- Player.updateCurrentState(Player(pid1, t0, Username("alice"), Active, None, t1))
      client <- fakeChessComClient(Map.empty)
      _      <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref    <- PlayerMatchRef.selectId(pid0)
      skip   <- PlayerRefSkip.selectId(pid0)
      taker  <- PlayerRefSkip.selectId(pid1)
    } yield assertTrue(ref.isEmpty, skip.isEmpty, taker.isDefined)
  }

  private def suitePlayerResolution = suite("player resolution")(
    testResolvesPlayerOnTeam1,
    testResolvesPlayerOnTeam2,
    testSkipsPlayerWithNoFinishedMatchWithBoard,
    testSkipsPlayerNotFoundInEitherTeam,
    testApiErrorForOnePlayerDoesNotBlockOthers,
    testLeavesPlayerHoldingNoNameAlone
  )

  private def testResolvesPlayerOnTeam1 = test("resolves player on team1") {
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
          s"player/alice/matches"   -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"     -> emptyPlayerMatchesJson,
          s"player/charlie/matches" -> emptyPlayerMatchesJson,
          s"match/$matchId1"        -> matchJson
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerMatchRef.selectId(pid0)
    } yield assertTrue(
      ref.isDefined,
      ref.get.matchId == ClubMatchId.wrap(matchId1),
      ref.get.isTeam1,
      ref.get.boardIdx == 3
    )
  }

  private def testResolvesPlayerOnTeam2 = test("resolves player on team2") {
    val matchJson = apiDailyMatchJson(
      matchId2,
      "some-club",
      "bobs-club",
      team1Players = List(("opponent2", 1)),
      team2Players = List(("bob", 5))
    )
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"   -> emptyPlayerMatchesJson,
          s"player/bob/matches"     -> apiPlayerMatchesJson(List((matchId2, Some(5)))),
          s"player/charlie/matches" -> emptyPlayerMatchesJson,
          s"match/$matchId2"        -> matchJson
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerMatchRef.selectId(pid1)
    } yield assertTrue(
      ref.isDefined,
      ref.get.matchId == ClubMatchId.wrap(matchId2),
      !ref.get.isTeam1,
      ref.get.boardIdx == 5
    )
  }

  private def testSkipsPlayerWithNoFinishedMatchWithBoard = test("skips player with no finished match with board") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"   -> apiPlayerMatchesJson(List((matchId1, None))),
          s"player/bob/matches"     -> emptyPlayerMatchesJson,
          s"player/charlie/matches" -> emptyPlayerMatchesJson
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerMatchRef.selectId(pid0)
    } yield assertTrue(ref.isEmpty)
  }

  private def testSkipsPlayerNotFoundInEitherTeam = test("skips player not found in either team") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "club-a",
      "club-b",
      team1Players = List(("stranger1", 1)),
      team2Players = List(("stranger2", 2))
    )
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"   -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"     -> emptyPlayerMatchesJson,
          s"player/charlie/matches" -> emptyPlayerMatchesJson,
          s"match/$matchId1"        -> matchJson
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerMatchRef.selectId(pid0)
    } yield assertTrue(ref.isEmpty)
  }

  private def testApiErrorForOnePlayerDoesNotBlockOthers = test("API error for one player does not block others") {
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
        responses = Map(
          s"player/alice/matches" -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
          s"player/bob/matches"   -> emptyPlayerMatchesJson,
          s"match/$matchId1"      -> matchJson
        ),
        failures = Set("charlie")
      )
      _          <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      aliceRef   <- PlayerMatchRef.selectId(pid0)
      charlieRef <- PlayerMatchRef.selectId(pid2)
    } yield assertTrue(
      aliceRef.isDefined,
      aliceRef.get.matchId == ClubMatchId.wrap(matchId1),
      charlieRef.isEmpty
    )
  }

  // ==========================================================================
  // Suite: club resolution
  // ==========================================================================

  private def suiteClubResolution = suite("club resolution")(
    testResolvesClubOnTeam1,
    testResolvesClubOnTeam2,
    testSkipsClubWithNoFinishedMatch,
    testSkipsClubNotFoundInEitherTeam,
    testDoesNotResolveClubUnderATakenName
  )

  // #254: a club whose name another club took keeps it as its display cache. Resolving under that would fetch the
  // taker's matches and record the taker's board side as this club's ref.
  private def testDoesNotResolveClubUnderATakenName = test("does not resolve a club under a name another club took") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "our-club",
      "some-club",
      team1Players = List(("player1", 1)),
      team2Players = List(("player2", 2))
    )
    for {
      _ <- seedDb
      _ <- Club.upsert(Club(clubId1, t0, clubSlug0, "Club 1", None, None, None))
      client <- fakeChessComClient(
        Map(
          s"club/our-club/matches" -> apiClubMatchesJson(List(matchId1)),
          s"match/$matchId1"       -> matchJson
        )
      )
      _     <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      loser <- ClubMatchRef.selectId(clubId0)
      taker <- ClubMatchRef.selectId(clubId1)
      skip  <- ClubRefSkip.selectId(clubId0)
    } yield assertTrue(loser.isEmpty, skip.isEmpty, taker.exists(_.isTeam1))
  }

  private def testResolvesClubOnTeam1 = test("resolves club on team1") {
    val matchJson = apiDailyMatchJson(
      matchId1,
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
          s"club/our-club/matches"   -> apiClubMatchesJson(List(matchId1)),
          s"club/other-club/matches" -> emptyClubMatchesJson,
          s"match/$matchId1"         -> matchJson
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- ClubMatchRef.selectId(clubId0)
    } yield assertTrue(
      ref.isDefined,
      ref.get.matchId == ClubMatchId.wrap(matchId1),
      ref.get.isTeam1
    )
  }

  private def testResolvesClubOnTeam2 = test("resolves club on team2") {
    val matchJson = apiDailyMatchJson(
      matchId1,
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
          s"club/our-club/matches"   -> emptyClubMatchesJson,
          s"club/other-club/matches" -> apiClubMatchesJson(List(matchId1)),
          s"match/$matchId1"         -> matchJson
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- ClubMatchRef.selectId(clubId1)
    } yield assertTrue(
      ref.isDefined,
      ref.get.matchId == ClubMatchId.wrap(matchId1),
      !ref.get.isTeam1
    )
  }

  private def testSkipsClubWithNoFinishedMatch = test("skips club with no finished match") {
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
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- ClubMatchRef.selectId(clubId0)
    } yield assertTrue(ref.isEmpty)
  }

  private def testSkipsClubNotFoundInEitherTeam = test("skips club not found in either team") {
    val matchJson = apiDailyMatchJson(
      matchId1,
      "club-x",
      "club-y",
      team1Players = List(("player1", 1)),
      team2Players = List(("player2", 2))
    )
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"   -> emptyPlayerMatchesJson,
          s"player/bob/matches"     -> emptyPlayerMatchesJson,
          s"player/charlie/matches" -> emptyPlayerMatchesJson,
          s"club/our-club/matches"  -> apiClubMatchesJson(List(matchId1)),
          s"match/$matchId1"        -> matchJson
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- ClubMatchRef.selectId(clubId0)
    } yield assertTrue(ref.isEmpty)
  }

  // ==========================================================================
  // Suite: tournament resolution
  // ==========================================================================

  private def suiteTournamentResolution = suite("tournament resolution")(
    testResolvesPlayerViaTournamentRound1,
    testSkipsTournamentWherePlayerNotFoundInRound1
  )

  private def testResolvesPlayerViaTournamentRound1 = test("resolves player via tournament round 1") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"       -> emptyPlayerMatchesJson,
          s"player/bob/matches"         -> emptyPlayerMatchesJson,
          s"player/charlie/matches"     -> emptyPlayerMatchesJson,
          s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List(("tourney-1", 5))),
          s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
          s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
          s"tournament/tourney-1/1"     -> apiTournamentRoundJson(List("other-player", "alice", "third-player"))
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(
      ref.isDefined,
      ref.get.tournamentSlug == TournamentSlug("tourney-1"),
      ref.get.playerIdx == 1 // index in round 1 players
    )
  }

  private def testSkipsTournamentWherePlayerNotFoundInRound1 = test("skips tournament where player not found in round 1") {
    for {
      _ <- seedDb
      client <- fakeChessComClient(
        Map(
          s"player/alice/matches"       -> emptyPlayerMatchesJson,
          s"player/bob/matches"         -> emptyPlayerMatchesJson,
          s"player/charlie/matches"     -> emptyPlayerMatchesJson,
          s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List(("tourney-1", 5))),
          s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
          s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
          s"tournament/tourney-1/1"     -> apiTournamentRoundJson(List("stranger1", "stranger2"))
        )
      )
      _   <- runPopulate(client, forceSkipped = false, upgradeRefs = false)
      ref <- PlayerTournamentRef.selectId(pid0)
    } yield assertTrue(ref.isEmpty)
  }
}
