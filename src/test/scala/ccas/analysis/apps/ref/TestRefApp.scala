package ccas.analysis.apps.ref

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.{sql, Transactor}
import zio.{durationInt, Chunk, Fiber, RIO, Ref, Scope, Semaphore, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, TournamentSlug, Username}
import ccas.utils.{CcasLogger, TestCcasLogger}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.{FreshSchemaLayer, SqlZioTypes}

object TestRefApp extends ZIOSpecDefault {

  // --- Timestamps ---

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  // --- IDs ---

  private val pid0 = PlayerId(300)
  private val pid1 = PlayerId(301)
  private val pid2 = PlayerId(302)

  private val clubId0      = ClubId(700)
  private val clubId1      = ClubId(701)
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

  private def apiDailyMatchJson(
    matchId: Long,
    team1Club: String,
    team2Club: String,
    team1Players: List[(String, Int)],
    team2Players: List[(String, Int)]
  ): String = {
    def playerJson(username: String, boardIdx: Int): String =
      s"""{
        "username": "$username",
        "stats": "https://api.chess.com/pub/player/$username/stats",
        "status": "basic",
        "played_as_white": "win",
        "played_as_black": "win",
        "board": "https://api.chess.com/pub/match/$matchId/$boardIdx"
      }"""

    def teamJson(club: String, players: List[(String, Int)], result: String): String = {
      val playersStr = players.map((u, b) => playerJson(u, b)).mkString(",")
      s"""{
        "@id": "https://api.chess.com/pub/club/$club",
        "name": "${club.capitalize}",
        "url": "https://www.chess.com/club/$club",
        "score": 10.0,
        "result": "$result",
        "players": [$playersStr],
        "fair_play_removals": []
      }"""
    }

    s"""{
      "@id": "https://api.chess.com/pub/match/$matchId",
      "name": "Match $matchId",
      "url": "https://www.chess.com/club/matches/$matchId",
      "status": "finished",
      "start_time": ${t0.getEpochSecond},
      "end_time": ${t0.plusSeconds(86400).getEpochSecond},
      "boards": ${(team1Players ++ team2Players).size},
      "settings": {
        "rules": "chess",
        "time_class": "daily",
        "time_control": "1/259200",
        "min_required_games": 0
      },
      "teams": {
        "team1": ${teamJson(team1Club, team1Players, "win")},
        "team2": ${teamJson(team2Club, team2Players, "lose")}
      }
    }"""
  }

  private def apiPlayerTournamentsJson(finished: List[String]): String = {
    val items = finished.map { slug =>
      s"""{
        "url": "https://www.chess.com/tournament/$slug",
        "@id": "https://api.chess.com/pub/tournament/$slug",
        "wins": 1, "losses": 0, "draws": 0,
        "points_awarded": 1.0, "placement": 1, "status": "eliminated", "total_players": 5
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
  ): RIO[Transactor, ChessComClient] =
    for {
      transactor <- ZIO.service[Transactor]
      semaphore  <- Semaphore.make(5)
      stateRef   <- Ref.make(ChessComClient.ThrottleState(5, 0, Vector.empty))
      reserveRef  <- Ref.make(Chunk.empty[Fiber.Runtime[Nothing, Nothing]])
      adjustMutex <- Semaphore.make(1)
      activeRef   <- Ref.make(0)
      bar         <- TestCcasLogger.noopBar
    } yield {
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "player" / string("username") / "tournaments" -> handler { (username: String, _: Request) =>
          if (failures.contains(username)) Response(status = Status.InternalServerError)
          else responses.get(s"player/$username/tournaments").fold(Response.json(emptyPlayerTournamentsJson))(Response.json(_))
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
        Method.GET / "pub" / "tournament" / string("slug") / int("round") -> handler { (slug: String, round: Int, _: Request) =>
          responses.get(s"tournament/$slug/$round").fold(Response(status = Status.NotFound))(Response.json(_))
        }
      )
      val driver = new ZClient.Driver[Any, Scope, Throwable] {
        override def request(
          version: Version,
          method: Method,
          url: URL,
          headers: Headers,
          body: Body,
          sslConfig: Option[ClientSSLConfig],
          proxy: Option[Proxy]
        )(implicit trace: zio.Trace): ZIO[Scope, Throwable, Response] =
          routes.runZIO(Request(method = method, url = url, headers = headers, body = body))

        override def socket[Env1 <: Any](
          version: Version,
          url: URL,
          headers: Headers,
          app: WebSocketApp[Env1]
        )(implicit
          trace: zio.Trace,
          ev: Scope =:= Scope
        ): ZIO[Env1 & Scope, Throwable, Response] =
          ZIO.die(new UnsupportedOperationException)
      }
      ChessComClient(
        ZClient.fromDriver(driver),
        transactor,
        Headers.empty,
        TestCcasLogger.noop,
        semaphore,
        stateRef,
        reserveRef,
        adjustMutex,
        activeRef,
        bar,
        ChessComClient.ThrottleConfig(5, 30.seconds, 1.second, 5.seconds, 20, 0.2, 10)
      )
    }

  // --- DB helpers ---

  private val playerIdByUsername: Map[String, Long] = Map(
    "alice" -> PlayerId.unwrap(pid0), "bob" -> PlayerId.unwrap(pid1), "charlie" -> PlayerId.unwrap(pid2)
  )

  private def apiPlayerJson(username: String, playerId: Long): String =
    s"""{"player_id":$playerId,"username":"$username","country":"https://api.chess.com/pub/country/XX",
        |"status":"premium","joined":1000000000,"last_online":1000000000,"followers":0,"is_streamer":false,"verified":false}""".stripMargin

  private val testPlayerIds = List(pid0, pid1, pid2)
  private val testClubIds   = List(clubId0, clubId1)

  private def seedDb: RIO[Transactor, Unit] =
    for {
      // Clean in FK-safe order
      _ <- PlayerMatchRef.deleteAll
      _ <- PlayerTournamentRef.deleteAll
      _ <- ClubMatchRef.deleteAll
      _ <- ZIO.foreachDiscard(testPlayerIds) { pid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run())
      }
      _ <- ZIO.foreachDiscard(testPlayerIds) { pid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- ZIO.foreachDiscard(testClubIds) { cid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM club WHERE club_id = $cid".update.run())
      }
      // Insert test data
      _ <- Player.insert(Player(pid0, t0))
      _ <- Player.insert(Player(pid1, t0))
      _ <- Player.insert(Player(pid2, t0))
      _ <- PlayerSnapshot.insert(
        PlayerSnapshot(pid0, t0, Username("alice"), ccas.api.misc.enums.PlayerStatusCategory.Active, None)
      )
      _ <- PlayerSnapshot.insert(
        PlayerSnapshot(pid1, t0, Username("bob"), ccas.api.misc.enums.PlayerStatusCategory.Active, None)
      )
      _ <- PlayerSnapshot.insert(
        PlayerSnapshot(pid2, t0, Username("charlie"), ccas.api.misc.enums.PlayerStatusCategory.Active, None)
      )
      _ <- Club.upsert(Club(clubId0, t0, clubSlug0, "Club 0"))
      _ <- Club.upsert(Club(clubId1, t0, clubSlug1, "Club 1"))
    } yield ()

  private def runPopulate(client: ChessComClient): RIO[Scope & Transactor, Unit] =
    RefApp.populate(outputDir = "_test").provideSomeLayer(ZLayer.succeed(client) ++ CcasLogger.live(showProgress = false))

  // --- Spec ---

  override def spec: Spec[Any, Throwable] = suite("TestRefApp")(
    suitePlayerResolution,
    suiteClubResolution,
    suiteTournamentResolution,
    suiteIteration,
    suiteFullPopulate
  ).provideShared(
    FreshSchemaLayer("test_match_ref_app", onInit = Tables.ensureTables),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: player resolution
  // ==========================================================================

  private def suitePlayerResolution = suite("player resolution")(
    test("resolves player on team1") {
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
        _   <- runPopulate(client)
        ref <- PlayerMatchRef.selectId(pid0)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == ClubMatchId.wrap(matchId1),
        ref.get.isTeam1,
        ref.get.boardIdx == 3
      )
    },
    test("resolves player on team2") {
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
        _   <- runPopulate(client)
        ref <- PlayerMatchRef.selectId(pid1)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == ClubMatchId.wrap(matchId2),
        !ref.get.isTeam1,
        ref.get.boardIdx == 5
      )
    },
    test("skips player with no finished match with board") {
      for {
        _ <- seedDb
        client <- fakeChessComClient(
          Map(
            s"player/alice/matches"   -> apiPlayerMatchesJson(List((matchId1, None))),
            s"player/bob/matches"     -> emptyPlayerMatchesJson,
            s"player/charlie/matches" -> emptyPlayerMatchesJson
          )
        )
        _   <- runPopulate(client)
        ref <- PlayerMatchRef.selectId(pid0)
      } yield assertTrue(ref.isEmpty)
    },
    test("skips player not found in either team") {
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
        _   <- runPopulate(client)
        ref <- PlayerMatchRef.selectId(pid0)
      } yield assertTrue(ref.isEmpty)
    },
    test("API error for one player does not block others") {
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
        _          <- runPopulate(client)
        aliceRef   <- PlayerMatchRef.selectId(pid0)
        charlieRef <- PlayerMatchRef.selectId(pid2)
      } yield assertTrue(
        aliceRef.isDefined,
        aliceRef.get.matchId == ClubMatchId.wrap(matchId1),
        charlieRef.isEmpty
      )
    }
  )

  // ==========================================================================
  // Suite: club resolution
  // ==========================================================================

  private def suiteClubResolution = suite("club resolution")(
    test("resolves club on team1") {
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
        _   <- runPopulate(client)
        ref <- ClubMatchRef.selectId(clubId0)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == ClubMatchId.wrap(matchId1),
        ref.get.isTeam1
      )
    },
    test("resolves club on team2") {
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
        _   <- runPopulate(client)
        ref <- ClubMatchRef.selectId(clubId1)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == ClubMatchId.wrap(matchId1),
        !ref.get.isTeam1
      )
    },
    test("skips club with no finished match") {
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
        _   <- runPopulate(client)
        ref <- ClubMatchRef.selectId(clubId0)
      } yield assertTrue(ref.isEmpty)
    },
    test("skips club not found in either team") {
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
        _   <- runPopulate(client)
        ref <- ClubMatchRef.selectId(clubId0)
      } yield assertTrue(ref.isEmpty)
    }
  )

  // ==========================================================================
  // Suite: tournament resolution
  // ==========================================================================

  private def suiteTournamentResolution = suite("tournament resolution")(
    test("resolves player via tournament round 1") {
      for {
        _ <- seedDb
        client <- fakeChessComClient(
          Map(
            s"player/alice/matches"       -> emptyPlayerMatchesJson,
            s"player/bob/matches"         -> emptyPlayerMatchesJson,
            s"player/charlie/matches"     -> emptyPlayerMatchesJson,
            s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List("tourney-1")),
            s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
            s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
            s"tournament/tourney-1/1"     -> apiTournamentRoundJson(List("other-player", "alice", "third-player"))
          )
        )
        _   <- runPopulate(client)
        ref <- PlayerTournamentRef.selectId(pid0)
      } yield assertTrue(
        ref.isDefined,
        ref.get.tournamentSlug == TournamentSlug("tourney-1"),
        ref.get.playerIdx == 1 // index in round 1 players
      )
    },
    test("skips tournament where player not found in round 1") {
      for {
        _ <- seedDb
        client <- fakeChessComClient(
          Map(
            s"player/alice/matches"       -> emptyPlayerMatchesJson,
            s"player/bob/matches"         -> emptyPlayerMatchesJson,
            s"player/charlie/matches"     -> emptyPlayerMatchesJson,
            s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List("tourney-1")),
            s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
            s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
            s"tournament/tourney-1/1"     -> apiTournamentRoundJson(List("stranger1", "stranger2"))
          )
        )
        _   <- runPopulate(client)
        ref <- PlayerTournamentRef.selectId(pid0)
      } yield assertTrue(ref.isEmpty)
    }
  )

  // ==========================================================================
  // Suite: iteration and failed-URL cache
  // ==========================================================================

  private val matchId3 = 9003L

  private def suiteIteration = suite("iteration and failed-URL cache")(
    test("falls back to second match when first match 404s") {
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
            // match/$matchId1 not present → 404
          )
        )
        _   <- runPopulate(client)
        ref <- PlayerMatchRef.selectId(pid0)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == ClubMatchId.wrap(matchId2),
        ref.get.boardIdx == 5
      )
    },
    test("falls back to tournament when all matches fail") {
      for {
        _ <- seedDb
        client <- fakeChessComClient(
          Map(
            // alice has a match that will 404, then falls back to tournament
            s"player/alice/matches"       -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
            s"player/bob/matches"         -> emptyPlayerMatchesJson,
            s"player/charlie/matches"     -> emptyPlayerMatchesJson,
            s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List("tourney-1")),
            s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
            s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
            s"tournament/tourney-1/1"     -> apiTournamentRoundJson(List("alice", "other-player"))
            // match/$matchId1 not present → 404
          )
        )
        _        <- runPopulate(client)
        matchRef <- PlayerMatchRef.selectId(pid0)
        tournRef <- PlayerTournamentRef.selectId(pid0)
      } yield assertTrue(
        matchRef.isEmpty,
        tournRef.isDefined,
        tournRef.get.tournamentSlug == TournamentSlug("tourney-1"),
        tournRef.get.playerIdx == 0
      )
    },
    test("falls back to second tournament when first tournament round 404s") {
      for {
        _ <- seedDb
        client <- fakeChessComClient(
          Map(
            s"player/alice/matches"       -> emptyPlayerMatchesJson,
            s"player/bob/matches"         -> emptyPlayerMatchesJson,
            s"player/charlie/matches"     -> emptyPlayerMatchesJson,
            s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List("bad-tourney", "good-tourney")),
            s"player/bob/tournaments"     -> emptyPlayerTournamentsJson,
            s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
            // bad-tourney/1 not present → 404
            s"tournament/good-tourney/1"  -> apiTournamentRoundJson(List("alice", "someone"))
          )
        )
        _   <- runPopulate(client)
        ref <- PlayerTournamentRef.selectId(pid0)
      } yield assertTrue(
        ref.isDefined,
        ref.get.tournamentSlug == TournamentSlug("good-tourney"),
        ref.get.playerIdx == 0
      )
    },
    test("failed tournament URL is not retried for another player") {
      // Both alice and bob share bad-tourney (which 404s) and good-tourney
      for {
        _ <- seedDb
        client <- fakeChessComClient(
          Map(
            s"player/alice/matches"       -> emptyPlayerMatchesJson,
            s"player/bob/matches"         -> emptyPlayerMatchesJson,
            s"player/charlie/matches"     -> emptyPlayerMatchesJson,
            s"player/alice/tournaments"   -> apiPlayerTournamentsJson(List("bad-tourney", "good-tourney")),
            s"player/bob/tournaments"     -> apiPlayerTournamentsJson(List("bad-tourney", "good-tourney")),
            s"player/charlie/tournaments" -> emptyPlayerTournamentsJson,
            // bad-tourney/1 not present → 404 (should only be tried once across both players)
            s"tournament/good-tourney/1"  -> apiTournamentRoundJson(List("alice", "bob", "someone"))
          )
        )
        _        <- runPopulate(client)
        aliceRef <- PlayerTournamentRef.selectId(pid0)
        bobRef   <- PlayerTournamentRef.selectId(pid1)
      } yield assertTrue(
        aliceRef.isDefined,
        aliceRef.get.tournamentSlug == TournamentSlug("good-tourney"),
        bobRef.isDefined,
        bobRef.get.tournamentSlug == TournamentSlug("good-tourney")
      )
    },
    test("club resolution iterates past failed match") {
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
            // match/$matchId1 not present → 404
            s"match/$matchId3"         -> matchJson3
          )
        )
        _   <- runPopulate(client)
        ref <- ClubMatchRef.selectId(clubId0)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == ClubMatchId.wrap(matchId3),
        ref.get.isTeam1
      )
    }
  )

  // ==========================================================================
  // Suite: full populate
  // ==========================================================================

  private def suiteFullPopulate = suite("full populate")(
    test("resolves both players and clubs in one run") {
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
        _         <- runPopulate(client)
        playerRef <- PlayerMatchRef.selectId(pid0)
        clubRef   <- ClubMatchRef.selectId(clubId0)
      } yield assertTrue(
        playerRef.isDefined,
        playerRef.get.matchId == ClubMatchId.wrap(matchId1),
        clubRef.isDefined,
        clubRef.get.matchId == ClubMatchId.wrap(matchId1)
      )
    },
    test("already-resolved entities are not re-processed") {
      for {
        _ <- seedDb
        // Pre-seed match refs
        _ <- PlayerMatchRef.insert(PlayerMatchRef(pid0, ClubMatchId.wrap(matchId1), isLive = false, true, 3))
        _ <- ClubMatchRef.insert(ClubMatchRef(clubId0, ClubMatchId.wrap(matchId1), isLive = false, true))
        // Provide no API responses — if populate tries to fetch, it would get empty/404
        client <- fakeChessComClient(
          Map(
            s"player/bob/matches"      -> emptyPlayerMatchesJson,
            s"player/charlie/matches"  -> emptyPlayerMatchesJson,
            s"club/other-club/matches" -> emptyClubMatchesJson
          )
        )
        _         <- runPopulate(client)
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
  )
}
