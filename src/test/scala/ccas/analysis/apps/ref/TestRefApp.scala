package ccas.analysis.apps.ref

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

import com.augustnagro.magnum.sql
import zio.{RIO, Scope, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiDailyMatchJson
import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, TournamentSlug, Username}
import ccas.utils.TestCcasLogger
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

object TestRefApp extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRefApp")(
    suitePlayerResolution,
    suiteClubResolution,
    suiteTournamentResolution,
    suiteIteration,
    suiteFullPopulate,
    suiteSkipTracking,
    suiteForceSkipped,
    suiteTournamentSorting,
    suiteUpgrade
  ).provideShared(
    FreshSchemaLayer("test_match_ref_app", onInit = Tables.ensureTables),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // --- Timestamps ---

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  // --- IDs ---

  private val pid0 = PlayerId(300)
  private val pid1 = PlayerId(301)
  private val pid2 = PlayerId(302)

  private val clubId0   = ClubId(700)
  private val clubId1   = ClubId(701)
  private val clubSlug0 = ClubSlug("our-club")
  private val clubSlug1 = ClubSlug("other-club")

  private val matchId1 = 9001L
  private val matchId2 = 9002L

  // --- JSON builders ---

  private def apiPlayerMatchesJson(finished: List[(Long, Option[Int])]): String = {
    val items = finished.map { (matchId, boardOpt) =>
      val boardField = boardOpt.fold("")(b => s""", "board": "https://api.chess.com/pub/match/$matchId/$b"""")
      s"""{
        "name": "Match $matchId",
        "url": "https://www.chess.com/club/matches/$matchId",
        "@id": "https://api.chess.com/pub/match/$matchId",
        "club": "https://api.chess.com/pub/club/some-club"$boardField
      }"""
    }
    s"""{"finished": [${items.mkString(",")}], "in_progress": [], "registered": []}"""
  }

  private def apiClubMatchesJson(finishedIds: List[Long]): String = {
    val items = finishedIds.map { matchId =>
      s"""{
        "name": "Match $matchId",
        "@id": "https://api.chess.com/pub/match/$matchId",
        "opponent": "https://api.chess.com/pub/club/opponent-club",
        "time_class": "daily",
        "start_time": ${t0.getEpochSecond},
        "result": "win"
      }"""
    }
    s"""{"finished": [${items.mkString(",")}], "in_progress": [], "registered": []}"""
  }

  private def apiPlayerTournamentsJson(finished: List[(String, Int)]): String = {
    val items = finished.map { (slug, totalPlayers) =>
      s"""{
        "url": "https://www.chess.com/tournament/$slug",
        "@id": "https://api.chess.com/pub/tournament/$slug",
        "wins": 1, "losses": 0, "draws": 0,
        "points_awarded": 1.0, "placement": 1, "status": "eliminated", "total_players": $totalPlayers
      }"""
    }
    s"""{"finished": [${items.mkString(",")}], "in_progress": [], "registered": []}"""
  }

  private def apiTournamentRoundJson(players: List[String]): String = {
    val items = players.map(u => s"""{"username": "$u"}""")
    s"""{"groups": [], "players": [${items.mkString(",")}]}"""
  }

  private val emptyPlayerMatchesJson     = """{"finished": [], "in_progress": [], "registered": []}"""
  private val emptyPlayerTournamentsJson = """{"finished": [], "in_progress": [], "registered": []}"""
  private val emptyClubMatchesJson       = """{"finished": [], "in_progress": [], "registered": []}"""

  // --- Fake client ---

  private def fakeChessComClient(
    responses: Map[String, String],
    failures: Set[String] = Set.empty
  ): RIO[PostgresClient, ChessComClient] = {
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "player" / string("username") / "tournaments" -> handler { (username: String, _: Request) =>
        if (failures.contains(username)) Response(status = Status.InternalServerError)
        else
          responses.get(s"player/$username/tournaments").fold(Response.json(emptyPlayerTournamentsJson))(
            Response.json(_)
          )
      },
      Method.GET / "pub" / "player" / string("username") / "matches" -> handler { (username: String, _: Request) =>
        if (failures.contains(username)) Response(status = Status.InternalServerError)
        else responses.get(s"player/$username/matches").fold(Response.json(emptyPlayerMatchesJson))(Response.json(_))
      },
      Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
        if (failures.contains(username)) Response(status = Status.InternalServerError)
        else {
          val pid = playerIdByUsername.getOrElse(username.toLowerCase, 0L)
          Response.json(apiPlayerJson(username, pid))
        }
      },
      Method.GET / "pub" / "club" / string("club") / "matches" -> handler { (clubName: String, _: Request) =>
        responses.get(s"club/$clubName/matches").fold(Response.json(emptyClubMatchesJson))(Response.json(_))
      },
      Method.GET / "pub" / "match" / long("matchId") -> handler { (matchId: Long, _: Request) =>
        responses.get(s"match/$matchId").fold(Response(status = Status.NotFound))(Response.json(_))
      },
      Method.GET / "pub" / "tournament" / string("slug") / int("round") -> handler {
        (slug: String, round: Int, _: Request) =>
          responses.get(s"tournament/$slug/$round").fold(Response(status = Status.NotFound))(Response.json(_))
      }
    )
    TestChessComClientSupport.fakeClient(routes, permits = 5)
  }

  // --- DB helpers ---

  private val playerIdByUsername: Map[String, Long] = Map(
    "alice"   -> PlayerId.unwrap(pid0),
    "bob"     -> PlayerId.unwrap(pid1),
    "charlie" -> PlayerId.unwrap(pid2)
  )

  private def apiPlayerJson(username: String, playerId: Long): String =
    s"""{"player_id":$playerId,"username":"$username","country":"https://api.chess.com/pub/country/XX",
       |"status":"premium","joined":1000000000,"last_online":1000000000,"followers":0,"is_streamer":false,"verified":false}""".stripMargin

  private val testPlayerIds = List(pid0, pid1, pid2)
  private val testClubIds   = List(clubId0, clubId1)

  private def seedDb: RIO[PostgresClient, Unit] =
    for {
      // Clean in FK-safe order
      _ <- connectZIO(sql"DELETE FROM player_ref_skip".update.run())
      _ <- connectZIO(sql"DELETE FROM club_ref_skip".update.run())
      _ <- connectZIO(sql"DELETE FROM player_match_ref".update.run())
      _ <- connectZIO(sql"DELETE FROM player_tournament_ref".update.run())
      _ <- connectZIO(sql"DELETE FROM club_match_ref".update.run())
      _ <- ZIO.foreachDiscard(testPlayerIds) { pid =>
        connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run())
      }
      _ <- ZIO.foreachDiscard(testPlayerIds) { pid =>
        connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- ZIO.foreachDiscard(testClubIds) { cid =>
        connectZIO(sql"DELETE FROM club WHERE club_id = $cid".update.run())
      }
      // Insert test data
      _ <- Player.insert(Player(pid0, t0, Username("alice"), Active, None, t0))
      _ <- Player.insert(Player(pid1, t0, Username("bob"), Active, None, t0))
      _ <- Player.insert(Player(pid2, t0, Username("charlie"), Active, None, t0))
      _ <- Club.upsert(Club(clubId0, t0, clubSlug0, "Club 0", None))
      _ <- Club.upsert(Club(clubId1, t0, clubSlug1, "Club 1", None))
    } yield ()

  private def runPopulate(
    client: ChessComClient,
    forceSkipped: Boolean,
    upgradeRefs: Boolean
  ): RIO[Scope & PostgresClient, Unit] =
    RefApp
      .populate(forceSkipped = forceSkipped, upgradeRefs = upgradeRefs)
      .unit
      .provideSomeLayer(ZLayer.succeed(client) ++ ZLayer.succeed(TestCcasLogger.noop))

  // ==========================================================================
  // Suite: player resolution
  // ==========================================================================

  private def suitePlayerResolution = suite("player resolution")(
    testResolvesPlayerOnTeam1,
    testResolvesPlayerOnTeam2,
    testSkipsPlayerWithNoFinishedMatchWithBoard,
    testSkipsPlayerNotFoundInEitherTeam,
    testApiErrorForOnePlayerDoesNotBlockOthers
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
    testSkipsClubNotFoundInEitherTeam
  )

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
