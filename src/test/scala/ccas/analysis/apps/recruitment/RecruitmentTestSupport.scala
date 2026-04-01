package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.{sql, Transactor}
import zio.{durationInt, Promise, RIO, Ref, Scope, Semaphore, ZIO}
import zio.http.*

import ccas.analysis.tables.*
import ccas.api.club.ApiClubMatches
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes
import ccas.utils.TestCcasLogger

object RecruitmentTestSupport {

  // --- Timestamps ---

  object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(30))
    val t3: Instant = t0.plus(Duration.ofDays(60))
  }

  // --- IDs ---

  val clubId   = ClubId(500)
  val clubSlug = ClubSlug("test-club")
  val club     = Club(clubId, Times.t0, clubSlug, "Test Club")

  val sourceClubId    = ClubId(600)
  val intSourceClubId = ClubId(901)

  val blacklistClubId = ClubId(700)

  val pid0 = PlayerId(200)
  val pid1 = PlayerId(201)
  val pid2 = PlayerId(202)
  val pid3 = PlayerId(203)

  // --- JSON builders ---

  def apiPlayerJson(
    playerId: Long,
    username: String,
    status: String = "basic",
    joined: Long = Times.t0.getEpochSecond,
    country: String = "US"
  ): String = {
    val fields = List(
      s""""player_id": $playerId""",
      s""""username": "$username"""",
      s""""country": "https://api.chess.com/pub/country/$country"""",
      s""""status": "$status"""",
      s""""joined": $joined""",
      s""""last_online": $joined""",
      s""""followers": 0""",
      s""""is_streamer": false""",
      s""""verified": false""",
      s""""league": "wood""""
    )
    fields.mkString("{\n", ",\n", "\n}")
  }

  def apiClubJson(clubId: Long, slug: String, admins: List[String] = Nil): String = {
    val adminJson = admins.map(u => s""""https://api.chess.com/pub/player/$u"""").mkString("[", ",", "]")
    s"""{
       |  "@id": "https://api.chess.com/pub/club/$slug",
       |  "name": "Test Club",
       |  "club_id": $clubId,
       |  "country": "https://api.chess.com/pub/country/US",
       |  "average_daily_rating": 1200,
       |  "members_count": 10,
       |  "created": ${Times.t0.getEpochSecond},
       |  "last_activity": ${Times.t1.getEpochSecond},
       |  "visibility": "public",
       |  "join_request": "https://api.chess.com/pub/club/$slug/join",
       |  "admin": $adminJson,
       |  "description": "A test club"
       |}""".stripMargin
  }

  def apiClubMatchesJson(
    registeredIds: List[String] = Nil,
    finishedIds: List[Long] = Nil
  ): String = {
    val regs = registeredIds.map { id =>
      s"""{"name": "match", "@id": "$id", "opponent": "https://api.chess.com/pub/club/other", "time_class": "daily"}"""
    }
    val finished = finishedIds.map { matchId =>
      s"""{"name": "Match $matchId", "@id": "https://api.chess.com/pub/match/$matchId", "opponent": "https://api.chess.com/pub/club/other", "time_class": "daily", "start_time": ${Times.t0.getEpochSecond}, "result": "win"}"""
    }
    s"""{"finished": [${finished.mkString(",")}], "in_progress": [], "registered": [${regs.mkString(",")}]}"""
  }

  def apiPlayerMatchesJson(registeredIds: List[String] = Nil): String = {
    val regs = registeredIds.map { id =>
      s"""{"name": "match", "url": "https://chess.com/match/1", "@id": "$id", "club": "https://api.chess.com/pub/club/test", "board": "https://chess.com/board/1"}"""
    }
    s"""{"finished": [], "in_progress": [], "registered": [${regs.mkString(",")}]}"""
  }

  val emptyClubMatchesJson: String =
    """{"finished": [], "in_progress": [], "registered": []}"""

  val emptyPlayerMatchesJson: String =
    """{"finished": [], "in_progress": [], "registered": []}"""

  def apiPlayerClubsJson(clubs: List[String] = Nil): String = {
    val clubJsons = clubs.map { name =>
      s"""{"name": "$name", "last_activity": 0, "url": "https://api.chess.com/pub/club/$name", "joined": 0}"""
    }
    s"""{"clubs": [${clubJsons.mkString(",")}]}"""
  }

  def apiPlayerStatsJson(
    dailyElo: Int = 1200,
    timeoutPct: Double = 0.0,
    wins: Int = 100,
    losses: Int = 50,
    draws: Int = 10,
    timePerMove: Int = 86400
  ): String =
    s"""{
       |  "chess_daily": {
       |    "last": {"rating": $dailyElo, "date": 0, "rd": 0},
       |    "best": {"rating": $dailyElo, "date": 0, "game": "https://chess.com/game/1"},
       |    "record": {"win": $wins, "loss": $losses, "draw": $draws, "time_per_move": $timePerMove, "timeout_percent": $timeoutPct}
       |  },
       |  "chess960_daily": {
       |    "last": {"rating": 0, "date": 0, "rd": 0},
       |    "best": {"rating": 0, "date": 0, "game": "https://chess.com/game/1"},
       |    "record": {"win": 0, "loss": 0, "draw": 0, "time_per_move": 0, "timeout_percent": 0}
       |  },
       |  "chess_rapid": {
       |    "last": {"rating": 0, "date": 0, "rd": 0},
       |    "best": {"rating": 0, "date": 0, "game": "https://chess.com/game/1"},
       |    "record": {"win": 0, "loss": 0, "draw": 0}
       |  },
       |  "chess_blitz": {
       |    "last": {"rating": 0, "date": 0, "rd": 0},
       |    "best": {"rating": 0, "date": 0, "game": "https://chess.com/game/1"},
       |    "record": {"win": 0, "loss": 0, "draw": 0}
       |  },
       |  "chess_bullet": {
       |    "last": {"rating": 0, "date": 0, "rd": 0},
       |    "best": {"rating": 0, "date": 0, "game": "https://chess.com/game/1"},
       |    "record": {"win": 0, "loss": 0, "draw": 0}
       |  }
       |}""".stripMargin

  val emptyCurrentGamesJson: String = """{"games": []}"""

  val emptyArchiveJson: String = """{"games": []}"""

  def archiveGameJson(
    white: String,
    black: String,
    whiteResult: String = "win",
    blackResult: String = "checkmated",
    endTime: Long = Times.t2.getEpochSecond,
    matchUrl: Option[String] = None,
    timeClass: String = "daily"
  ): String = {
    val matchField = matchUrl.fold("")(u => s""", "match": "$u"""")
    s"""{
       |  "url": "https://www.chess.com/game/daily/1",
       |  "pgn": "",
       |  "time_control": "1/259200",
       |  "end_time": $endTime,
       |  "rated": true,
       |  "tcn": "",
       |  "uuid": "${java.util.UUID.randomUUID()}",
       |  "initial_setup": "",
       |  "fen": "",
       |  "start_time": ${endTime - 86400},
       |  "time_class": "$timeClass",
       |  "rules": "chess",
       |  "white": {
       |    "rating": 1500,
       |    "result": "$whiteResult",
       |    "@id": "https://api.chess.com/pub/player/$white",
       |    "username": "$white",
       |    "uuid": "${java.util.UUID.randomUUID()}"
       |  },
       |  "black": {
       |    "rating": 1500,
       |    "result": "$blackResult",
       |    "@id": "https://api.chess.com/pub/player/$black",
       |    "username": "$black",
       |    "uuid": "${java.util.UUID.randomUUID()}"
       |  }$matchField
       |}""".stripMargin
  }

  def archiveJson(games: List[String]): String =
    s"""{"games": [${games.mkString(",")}]}"""

  def apiPlayerMatchesJsonWithFinished(
    finishedMatches: List[(Long, Int)] = Nil,
    registeredIds: List[String] = Nil
  ): String = {
    val finished = finishedMatches.map { (matchId, boardIdx) =>
      s"""{"name": "match", "url": "https://chess.com/match/$matchId", "@id": "https://api.chess.com/pub/match/$matchId", "club": "https://api.chess.com/pub/club/some-club", "board": "https://api.chess.com/pub/match/$matchId/$boardIdx"}"""
    }
    val regs = registeredIds.map { id =>
      s"""{"name": "match", "url": "https://chess.com/match/1", "@id": "$id", "club": "https://api.chess.com/pub/club/test", "board": "https://chess.com/board/1"}"""
    }
    s"""{"finished": [${finished.mkString(",")}], "in_progress": [], "registered": [${regs.mkString(",")}]}"""
  }

  def apiDailyMatchJson(
    matchId: Long,
    team1Club: String,
    team2Club: String,
    team1Players: List[(String, Int)],
    team2Players: List[(String, Int)]
  ): String = {
    def playerJson(username: String, boardIdx: Int): String =
      s"""{"username": "$username", "stats": "https://api.chess.com/pub/player/$username/stats", "status": "basic", "played_as_white": "win", "played_as_black": "win", "board": "https://api.chess.com/pub/match/$matchId/$boardIdx"}"""
    def teamJson(club: String, players: List[(String, Int)], result: String): String = {
      val playersStr = players.map((u, b) => playerJson(u, b)).mkString(",")
      s"""{"@id": "https://api.chess.com/pub/club/$club", "name": "${club.capitalize}", "url": "https://www.chess.com/club/$club", "score": 10.0, "result": "$result", "players": [$playersStr], "fair_play_removals": []}"""
    }
    s"""{"@id": "https://api.chess.com/pub/match/$matchId", "name": "Match $matchId", "url": "https://www.chess.com/club/matches/$matchId", "status": "finished", "start_time": ${Times.t0.getEpochSecond}, "end_time": ${Times.t1.getEpochSecond}, "boards": ${(team1Players ++ team2Players).size}, "settings": {"rules": "chess", "time_class": "daily", "time_control": "1/259200", "min_required_games": 0}, "teams": {"team1": ${teamJson(
        team1Club,
        team1Players,
        "win"
      )}, "team2": ${teamJson(team2Club, team2Players, "lose")}}}"""
  }

  def apiClubMembersJson(members: List[(String, Long)]): String = {
    val memberJsons = members.map { (username, joined) =>
      s"""{"username": "$username", "joined": $joined}"""
    }
    s"""{"weekly": [${memberJsons.mkString(",")}], "monthly": [], "all_time": []}"""
  }

  // --- Fake HTTP clients ---

  def fakeChessComClient(
    responses: Map[String, String],
    failures: Set[String] = Set.empty
  ): RIO[Transactor, ChessComClient] =
    for {
      transactor    <- ZIO.service[Transactor]
      stateRef      <- Ref.make(ChessComClient.ThrottleState(1, 0, Vector.empty))
      activeRef     <- Ref.make(0)
      rateLimitGate <- Semaphore.make(1)
      lastReqRef    <- Ref.make(0L)
      ema           <- Ref.make(0.0)
      bar           <- TestCcasLogger.noopBar
      stats         <- Ref.make(ChessComClient.StatsAccumulator())
    } yield {
      val routes: Routes[Any, Response] = Routes(
        // Player stats endpoint
        Method.GET / "pub" / "player" / string("username") / "stats" -> handler { (username: String, _: Request) =>
          responses.get(s"player/$username/stats").fold(Response.json(apiPlayerStatsJson()))(Response.json(_))
        },
        // Player clubs endpoint
        Method.GET / "pub" / "player" / string("username") / "clubs" -> handler { (username: String, _: Request) =>
          responses.get(s"player/$username/clubs").fold(Response.json(apiPlayerClubsJson()))(Response.json(_))
        },
        // Player matches endpoint
        Method.GET / "pub" / "player" / string("username") / "matches" -> handler { (username: String, _: Request) =>
          responses.get(s"player/$username/matches").fold(Response.json(emptyPlayerMatchesJson))(Response.json(_))
        },
        // Player current games endpoint
        Method.GET / "pub" / "player" / string("username") / "games" -> handler { (username: String, _: Request) =>
          responses.get(s"player/$username/games").fold(Response.json(emptyCurrentGamesJson))(Response.json(_))
        },
        // Player archive endpoint (year/month)
        Method.GET / "pub" / "player" / string("username") / "games" / string("year") / string("month") -> handler {
          (username: String, year: String, month: String, _: Request) =>
            responses.get(s"player/$username/games/$year/$month")
              .fold(Response.json(emptyArchiveJson))(Response.json(_))
        },
        // Player endpoint
        Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
          if (failures.contains(username)) { Response(status = Status.NotFound) }
          else { responses.get(s"player/$username").fold(Response(status = Status.NotFound))(Response.json(_)) }
        },
        // Club matches endpoint
        Method.GET / "pub" / "club" / string("club") / "matches" -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName/matches").fold(Response.json(emptyClubMatchesJson))(Response.json(_))
        },
        // Club members endpoint
        Method.GET / "pub" / "club" / string("club") / "members" -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName/members").fold(Response(status = Status.NotFound))(Response.json(_))
        },
        // Club endpoint
        Method.GET / "pub" / "club" / string("club") -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName").fold(Response(status = Status.NotFound))(Response.json(_))
        },
        // Match endpoint (for ref resolution)
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
      val refs = ChessComClient.ThrottleRefs(
        stateRef,
        activeRef,
        rateLimitGate,
        lastReqRef,
        ema
      )
      ChessComClient(
        ZClient.fromDriver(driver),
        transactor,
        Headers.empty,
        TestCcasLogger.noop,
        refs,
        stats,
        bar,
        ChessComClient.ThrottleConfig(1, 30.seconds, 5.seconds, 1.second, 5.seconds, 10.seconds, 20, 0.2, 10)
      )
    }

  /** A variant of fakeChessComClient where the Nth player profile request blocks. After `blockAfterN` successful player
    * profile fetches, the next fetch completes `reached` and then awaits `gate` before responding. This ensures exactly
    * `blockAfterN` candidates have had their profiles fetched (and thus fully evaluated, since concurrency = 1) before
    * the block occurs, regardless of Set iteration order.
    */
  def fakeChessComClientWithBlock(
    responses: Map[String, String],
    blockAfterN: Int,
    reached: Promise[Nothing, Unit],
    gate: Promise[Nothing, Unit]
  ): RIO[Transactor, ChessComClient] =
    for {
      transactor    <- ZIO.service[Transactor]
      stateRef      <- Ref.make(ChessComClient.ThrottleState(5, 0, Vector.empty))
      activeRef     <- Ref.make(0)
      rateLimitGate <- Semaphore.make(1)
      lastReqRef    <- Ref.make(0L)
      ema           <- Ref.make(0.0)
      bar           <- TestCcasLogger.noopBar
      stats         <- Ref.make(ChessComClient.StatsAccumulator())
      playerCount   <- Ref.make(0)
    } yield {
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "player" / string("username") / "stats" -> handler { (username: String, _: Request) =>
          responses.get(s"player/$username/stats").fold(Response.json(apiPlayerStatsJson()))(Response.json(_))
        },
        Method.GET / "pub" / "player" / string("username") / "clubs" -> handler { (username: String, _: Request) =>
          responses.get(s"player/$username/clubs").fold(Response.json(apiPlayerClubsJson()))(Response.json(_))
        },
        Method.GET / "pub" / "player" / string("username") / "matches" -> handler { (username: String, _: Request) =>
          responses.get(s"player/$username/matches").fold(Response.json(emptyPlayerMatchesJson))(Response.json(_))
        },
        Method.GET / "pub" / "player" / string("username") / "games" -> handler { (username: String, _: Request) =>
          responses.get(s"player/$username/games").fold(Response.json(emptyCurrentGamesJson))(Response.json(_))
        },
        Method.GET / "pub" / "player" / string("username") / "games" / string("year") / string("month") -> handler {
          (username: String, year: String, month: String, _: Request) =>
            responses.get(s"player/$username/games/$year/$month")
              .fold(Response.json(emptyArchiveJson))(Response.json(_))
        },
        Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
          val resp = responses.get(s"player/$username").fold(Response(status = Status.NotFound))(Response.json(_))
          playerCount.getAndUpdate(_ + 1).flatMap { count =>
            if (count >= blockAfterN)
              reached.succeed(()) *> gate.await.as(resp)
            else
              ZIO.succeed(resp)
          }
        }.sandbox,
        Method.GET / "pub" / "club" / string("club") / "matches" -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName/matches").fold(Response.json(emptyClubMatchesJson))(Response.json(_))
        },
        Method.GET / "pub" / "club" / string("club") / "members" -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName/members").fold(Response(status = Status.NotFound))(Response.json(_))
        },
        Method.GET / "pub" / "club" / string("club") -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName").fold(Response(status = Status.NotFound))(Response.json(_))
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
      val refs = ChessComClient.ThrottleRefs(
        stateRef,
        activeRef,
        rateLimitGate,
        lastReqRef,
        ema
      )
      ChessComClient(
        ZClient.fromDriver(driver),
        transactor,
        Headers.empty,
        TestCcasLogger.noop,
        refs,
        stats,
        bar,
        ChessComClient.ThrottleConfig(5, 30.seconds, 5.seconds, 1.second, 5.seconds, 10.seconds, 20, 0.2, 10)
      )
    }

  // --- DB seeders ---

  def seedCriteria(criteria: RecruitmentCriteria): RIO[Transactor, Long] =
    for {
      criteriaId <- RecruitmentCriteria.insert(criteria)
      _          <- RecruitmentAlias.insert(RecruitmentAlias(clubId, "default", Instant.now(), criteriaId))
    } yield criteriaId

  def seedDb: RIO[Transactor, Unit] =
    for {
      // Clean up test data
      _ <- RecruitmentCandidate.deleteAll
      _ <- RecruitmentRun.deleteAll
      _ <- RecruitmentBlacklist.deleteAll
      _ <- RecruitmentAlias.deleteAll
      _ <- RecruitmentCriteria.deleteAll
      _ <- PlayerRecruitmentCache.deleteAll
      _ <- ApiFetchFailure.deleteAll
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_member WHERE club_id = $clubId".update.run())
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_member WHERE club_id = $sourceClubId".update.run())
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_member WHERE club_id = $intSourceClubId".update.run())
      _ <- ZIO.foreachDiscard(
        List(blacklistClubId, ClubId(701), ClubId(702), ClubId(777), ClubId(801), ClubId(802), intSourceClubId)
      ) { cid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM club WHERE club_id = $cid".update.run())
      }
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_match_ref WHERE club_id = $clubId".update.run())
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_match_board WHERE match_id IN (8001, 8002, 9001)".update.run())
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_match WHERE match_id IN (8001, 8002, 9001)".update.run())
      _ <- ZIO.foreachDiscard(
        List(
          PlayerId(199),
          pid0,
          pid1,
          pid2,
          pid3,
          PlayerId(210),
          PlayerId(211),
          PlayerId(220),
          PlayerId(221),
          PlayerId(222),
          PlayerId(223),
          PlayerId(250),
          PlayerId(251),
          PlayerId(252),
          PlayerId(253),
          PlayerId(300),
          PlayerId(301),
          PlayerId(302),
          PlayerId(303),
          PlayerId(304),
          PlayerId(999)
        )
      ) { pid =>
        SqlZioTypes.connectZIO(
          sql"DELETE FROM club_match_board WHERE team1_player_id = $pid OR team2_player_id = $pid".update.run()
        ) *>
          SqlZioTypes.connectZIO(sql"DELETE FROM player_match_ref WHERE player_id = $pid".update.run()) *>
          SqlZioTypes.connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run()) *>
          SqlZioTypes.connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- Club.upsert(club)
    } yield ()

  def seedPlayer(playerId: PlayerId): RIO[Transactor, Unit] = {
    val username = Username.wrap(s"player_${PlayerId.unwrap(playerId)}")
    SqlZioTypes.connectZIO {
      sql"""INSERT INTO player (player_id, joined, username, status, title, since)
            VALUES ($playerId, ${Times.t0}, $username, 'Active', NULL, ${Times.t0})
            ON CONFLICT (player_id) DO NOTHING""".update.run()
    }.unit
  }

  /** Test-side helper that calls real production code: builds a RunContext, filter chain, and loops evaluateCandidate —
    * matching what the explore loop does.
    */
  def evalCandidates(
    client: ChessComClient,
    runId: Long,
    candidates: List[Username],
    criteria: RecruitmentCriteria,
    target: Int = 30
  ): RIO[Transactor, List[Username]] =
    for {
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubSlug))
      targetMatchIds = (clubMatches.registered.map(_.`@id`) ++ clubMatches.inProgress.map(_.`@id`)).toSet
      formerMemberIds <-
        if (criteria.excludeFormerMembers)
          ClubMember.selectClubFormer(clubId).map(_.map(_.playerId).toSet)
        else ZIO.succeed(Set.empty[PlayerId])
      discoveredClubs     <- Ref.make(Set.empty[ClubSlug])
      discoveredOpponents <- Ref.make(Set.empty[Username])
      excludedSlugs <- ZIO.foreach(criteria.excludeClubs)(Club.selectId(_))
        .map(_.flatten.map(_.slug).toSet)
      runCtx = RunContext(
        client,
        criteria,
        clubId,
        "default",
        targetMatchIds,
        formerMemberIds,
        excludedSlugs,
        Instant.now(),
        discoveredClubs,
        discoveredOpponents
      )
      filters = RecruitmentFilters.buildFilterChain(criteria)
      revInvited <- ZIO.foldLeft(candidates)(List.empty[Username]) { case (invited, username) =>
        if (invited.size >= target) ZIO.succeed(invited)
        else
          RecruitmentFilters.evaluateCandidate(runId, username, runCtx, filters).map { outcome =>
            if (outcome == CandidateOutcome.Invited) username :: invited else invited
          }
      }
    } yield revInvited.reverse

  def makeCriteria(
    excludeClubs: List[ClubId] = Nil,
    excludeFormerMembers: Boolean = false,
    daysSinceRejected: Option[Int] = None
  ): RecruitmentCriteria =
    RecruitmentCriteria(
      criteriaId = 0,
      minDaysSinceRegistration = None,
      daysSinceLastInvited = None,
      daysSinceRejected = daysSinceRejected,
      nationalityExclude = false,
      nationalityCountries = Nil,
      excludeClubs = excludeClubs,
      maxClubs = None,
      excludeSourceAdmins = true,
      excludeFormerMembers = excludeFormerMembers,
      dailyMinElo = None,
      dailyMaxElo = None,
      dailyMinScoreRate = None,
      dailyMaxScoreRate = None,
      dailyMinGamesFinished = None,
      dailyMinTmGamesFinished = None,
      dailyMaxTimeoutPercent = None,
      dailyMaxTmTimeoutPercent = None,
      dailyMaxHoursPerMove = None,
      dailyMinOngoingGames = None,
      dailyMaxOngoingGames = None,
      dailyMinOngoingTeamMatches = None
    )
}
