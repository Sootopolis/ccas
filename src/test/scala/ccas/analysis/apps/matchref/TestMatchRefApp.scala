package ccas.analysis.apps.matchref

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.{sql, Transactor}
import zio.{RIO, Ref, Scope, Semaphore, UIO, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubUrlName, PlayerId, Username}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.{FreshSchemaLayer, SqlZioTypes}

object TestMatchRefApp extends ZIOSpecDefault {

  // --- Timestamps ---

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  // --- IDs ---

  private val pid0 = PlayerId(300)
  private val pid1 = PlayerId(301)
  private val pid2 = PlayerId(302)

  private val clubId0      = ClubId(700)
  private val clubId1      = ClubId(701)
  private val clubUrlName0 = ClubUrlName("our-club")
  private val clubUrlName1 = ClubUrlName("other-club")

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

  private val emptyPlayerMatchesJson = """{"finished": [], "in_progress": [], "registered": []}"""
  private val emptyClubMatchesJson   = """{"finished": [], "in_progress": [], "registered": []}"""

  // --- Fake client ---

  private def fakeChessComClient(
    responses: Map[String, String],
    failures: Set[String] = Set.empty
  ): UIO[ChessComClient] =
    (for {
      semaphore <- Semaphore.make(5)
      mutex     <- Semaphore.make(1)
      throttled <- Ref.make(false)
    } yield (semaphore, mutex, throttled)).map { (semaphore, mutex, throttled) =>
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "player" / string("username") / "matches" -> handler { (username: String, _: Request) =>
          if (failures.contains(username)) Response(status = Status.InternalServerError)
          else responses.get(s"player/$username/matches").fold(Response.json(emptyPlayerMatchesJson))(Response.json(_))
        },
        Method.GET / "pub" / "club" / string("club") / "matches" -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName/matches").fold(Response.json(emptyClubMatchesJson))(Response.json(_))
        },
        Method.GET / "pub" / "match" / long("matchId") -> handler { (matchId: Long, _: Request) =>
          responses.get(s"match/$matchId").fold(Response(status = Status.NotFound))(Response.json(_))
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
        Headers.empty,
        semaphore,
        mutex,
        throttled,
        zio.Duration.fromSeconds(30)
      )
    }

  // --- DB helpers ---

  private val testPlayerIds = List(pid0, pid1, pid2)
  private val testClubIds   = List(clubId0, clubId1)

  private def seedDb: RIO[Transactor, Unit] =
    for {
      // Clean in FK-safe order
      _ <- PlayerMatchRef.deleteAll
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
      _ <- Club.upsert(Club(clubId0, t0, clubUrlName0))
      _ <- Club.upsert(Club(clubId1, t0, clubUrlName1))
    } yield ()

  private def runPopulate(client: ChessComClient): RIO[Transactor, Unit] =
    MatchRefApp.populate.provideSomeLayer(ZLayer.succeed(client))

  // --- Spec ---

  override def spec: Spec[Any, Throwable] = suite("TestMatchRefApp")(
    suitePlayerResolution,
    suiteClubResolution,
    suiteFullPopulate
  ).provideShared(
    FreshSchemaLayer("test_match_ref_app", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

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
        ref.get.teamIdx == 1,
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
        ref.get.teamIdx == 2,
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
        ref.get.teamIdx == 1
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
        ref.get.teamIdx == 2
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
        _ <- PlayerMatchRef.upsert(PlayerMatchRef(pid0, ClubMatchId.wrap(matchId1), 1, 3))
        _ <- ClubMatchRef.upsert(ClubMatchRef(clubId0, ClubMatchId.wrap(matchId1), 1))
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
        playerRef.get.teamIdx == 1,
        playerRef.get.boardIdx == 3,
        clubRef.isDefined,
        clubRef.get.matchId == ClubMatchId.wrap(matchId1),
        clubRef.get.teamIdx == 1
      )
    }
  )
}
