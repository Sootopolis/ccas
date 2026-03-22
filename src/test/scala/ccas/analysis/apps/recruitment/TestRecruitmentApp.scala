package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Promise, RIO, Ref, Scope, Semaphore, UIO, ZIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.*
import ccas.api.club.ApiClubMatches
import ccas.api.misc.subtypes.{ClubId, ClubUrlName, PlayerId, Username}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.{FreshSchemaLayer, SqlZioTypes}
import ccas.utils.sql.DbCodecs.given

object TestRecruitmentApp extends ZIOSpecDefault {

  // --- Timestamps ---

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(30))
    val t3: Instant = t0.plus(Duration.ofDays(60))
  }

  // --- IDs ---

  private val clubId      = ClubId(500)
  private val clubUrlName = ClubUrlName("test-club")
  private val club        = Club(clubId, Times.t0, clubUrlName)

  private val sourceClubId    = ClubId(600)
  private val intSourceClubId = ClubId(901)

  private val pid0 = PlayerId(200)
  private val pid1 = PlayerId(201)
  private val pid2 = PlayerId(202)
  private val pid3 = PlayerId(203)

  // --- Helpers ---

  private def apiPlayerJson(
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

  private def apiClubJson(clubId: Long, urlName: String, admins: List[String] = Nil): String = {
    val adminJson = admins.map(u => s""""https://api.chess.com/pub/player/$u"""").mkString("[", ",", "]")
    s"""{
       |  "@id": "https://api.chess.com/pub/club/$urlName",
       |  "name": "Test Club",
       |  "club_id": $clubId,
       |  "country": "https://api.chess.com/pub/country/US",
       |  "average_daily_rating": 1200,
       |  "members_count": 10,
       |  "created": ${Times.t0.getEpochSecond},
       |  "last_activity": ${Times.t1.getEpochSecond},
       |  "visibility": "public",
       |  "join_request": "https://api.chess.com/pub/club/$urlName/join",
       |  "admin": $adminJson,
       |  "description": "A test club"
       |}""".stripMargin
  }

  private def apiClubMatchesJson(registeredIds: List[String] = Nil): String = {
    val regs = registeredIds.map { id =>
      s"""{"name": "match", "@id": "$id", "opponent": "https://api.chess.com/pub/club/other", "time_class": "daily"}"""
    }
    s"""{"finished": [], "in_progress": [], "registered": [${regs.mkString(",")}]}"""
  }

  private def apiPlayerMatchesJson(registeredIds: List[String] = Nil): String = {
    val regs = registeredIds.map { id =>
      s"""{"name": "match", "url": "https://chess.com/match/1", "@id": "$id", "club": "https://api.chess.com/pub/club/test", "board": "https://chess.com/board/1"}"""
    }
    s"""{"finished": [], "in_progress": [], "registered": [${regs.mkString(",")}]}"""
  }

  private val emptyClubMatchesJson: String =
    """{"finished": [], "in_progress": [], "registered": []}"""

  private val emptyPlayerMatchesJson: String =
    """{"finished": [], "in_progress": [], "registered": []}"""

  private def apiPlayerClubsJson(clubs: List[String] = Nil): String = {
    val clubJsons = clubs.map { name =>
      s"""{"name": "$name", "last_activity": 0, "url": "https://api.chess.com/pub/club/$name", "joined": 0}"""
    }
    s"""{"clubs": [${clubJsons.mkString(",")}]}"""
  }

  private def apiPlayerStatsJson(
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

  private val emptyCurrentGamesJson: String = """{"games": []}"""

  private val emptyArchiveJson: String = """{"games": []}"""

  private def archiveGameJson(
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

  private def archiveJson(games: List[String]): String =
    s"""{"games": [${games.mkString(",")}]}"""

  private def apiClubMembersJson(members: List[(String, Long)]): String = {
    val memberJsons = members.map { (username, joined) =>
      s"""{"username": "$username", "joined": $joined}"""
    }
    s"""{"weekly": [${memberJsons.mkString(",")}], "monthly": [], "all_time": []}"""
  }

  private def fakeChessComClient(
    responses: Map[String, String],
    failures: Set[String] = Set.empty
  ): UIO[ChessComClient] =
    (for {
      semaphore <- Semaphore.make(1)
      mutex     <- Semaphore.make(1)
      throttled <- Ref.make(false)
    } yield (semaphore, mutex, throttled)).map { (semaphore, mutex, throttled) =>
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

  /** A variant of fakeChessComClient where the Nth player profile request blocks. After `blockAfterN` successful player
    * profile fetches, the next fetch completes `reached` and then awaits `gate` before responding. This ensures exactly
    * `blockAfterN` candidates have had their profiles fetched (and thus fully evaluated, since concurrency = 1) before
    * the block occurs, regardless of Set iteration order.
    */
  private def fakeChessComClientWithBlock(
    responses: Map[String, String],
    blockAfterN: Int,
    reached: Promise[Nothing, Unit],
    gate: Promise[Nothing, Unit]
  ): UIO[ChessComClient] =
    (for {
      semaphore   <- Semaphore.make(1)
      mutex       <- Semaphore.make(1)
      throttled   <- Ref.make(false)
      playerCount <- Ref.make(0)
    } yield (semaphore, mutex, throttled, playerCount)).map { (semaphore, mutex, throttled, playerCount) =>
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
      ChessComClient(
        ZClient.fromDriver(driver),
        Headers.empty,
        semaphore,
        mutex,
        throttled,
        zio.Duration.fromSeconds(30)
      )
    }

  private def seedCriteria(criteria: RecruitmentCriteria): RIO[Transactor, Long] =
    for {
      criteriaId <- RecruitmentCriteria.insert(criteria)
      _          <- RecruitmentAlias.insert(RecruitmentAlias(clubId, "default", Instant.now(), criteriaId))
    } yield criteriaId

  private def seedDb: RIO[Transactor, Unit] =
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
        List(blacklistClubId, ClubId(701), ClubId(702), ClubId(801), ClubId(802), intSourceClubId)
      ) { cid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM club WHERE club_id = $cid".update.run())
      }
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
          PlayerId(304)
        )
      ) { pid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run()) *>
          SqlZioTypes.connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- Club.upsert(club)
    } yield ()

  private def seedPlayer(playerId: PlayerId): RIO[Transactor, Unit] =
    SqlZioTypes.connectZIO {
      sql"""INSERT INTO player (player_id, joined)
            VALUES ($playerId, ${Times.t0})
            ON CONFLICT (player_id) DO NOTHING""".update.run()
    }.unit

  /** Test-side helper that calls real production code: builds a RunContext, filter chain, and loops evaluateCandidate —
    * matching what the explore loop does.
    */
  private def evalCandidates(
    client: ChessComClient,
    runId: Long,
    candidates: List[Username],
    criteria: RecruitmentCriteria,
    target: Int = 30
  ): RIO[Transactor, List[Username]] =
    for {
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubUrlName))
      targetMatchIds = (clubMatches.registered.map(_.`@id`) ++ clubMatches.inProgress.map(_.`@id`)).toSet
      formerMemberIds <-
        if (criteria.excludeFormerMembers)
          ClubMember.selectClubFormer(clubId).map(_.map(_.playerId).toSet)
        else ZIO.succeed(Set.empty[PlayerId])
      discoveredClubs     <- Ref.make(Set.empty[ClubUrlName])
      discoveredOpponents <- Ref.make(Set.empty[Username])
      runCtx = RecruitmentApp.RunContext(
        client,
        criteria,
        clubId,
        "default",
        targetMatchIds,
        formerMemberIds,
        Instant.now(),
        discoveredClubs,
        discoveredOpponents
      )
      filters = RecruitmentApp.buildFilterChain(criteria)
      revInvited <- ZIO.foldLeft(candidates)(List.empty[Username]) { case (invited, username) =>
        if (invited.size >= target) ZIO.succeed(invited)
        else
          RecruitmentApp.evaluateCandidate(runId, username, runCtx, filters).map { outcome =>
            if (outcome == CandidateOutcome.Invited) username :: invited else invited
          }
      }
    } yield revInvited.reverse

  private def makeCriteria(
    excludeClubs: List[String] = Nil,
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
      dailyMinGamesFinished = None,
      dailyMinTmGamesFinished = None,
      dailyMaxTimeoutPercent = None,
      dailyMaxTmTimeoutPercent = None,
      dailyMaxHoursPerMove = None,
      dailyMinOngoingGames = None,
      dailyMaxOngoingGames = None,
      dailyMinOngoingTeamMatches = None
    )

  // --- Spec ---

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentApp")(
    suiteCriteriaHelpers,
    suiteDbCrud,
    suiteEvaluateCandidates,
    suiteFilterChain,
    suiteBlacklist,
    suiteBlacklistApp,
    suiteCacheFilters,
    suiteFullWorkflow,
    suiteExploreMode,
    suiteReport
  ).provideShared(
    FreshSchemaLayer("test_recruitment_app", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  // ==========================================================================
  // Suite: Criteria helper methods (pure)
  // ==========================================================================

  private def suiteCriteriaHelpers = suite("criteria helpers")(
    test("excludeClubNames wraps strings to ClubUrlName") {
      val criteria = makeCriteria(excludeClubs = List("club-x"))
      assertTrue(
        criteria.excludeClubNames == List(ClubUrlName("club-x"))
      )
    },
    test("defaultDaily returns expected field values") {
      val criteria = RecruitmentCriteria.defaultDaily
      assertTrue(
        criteria.criteriaId == 0,
        criteria.minDaysSinceRegistration.contains(90),
        criteria.daysSinceLastInvited.contains(180),
        criteria.daysSinceRejected.contains(30),
        !criteria.nationalityExclude,
        criteria.nationalityCountries.isEmpty,
        criteria.excludeClubs.isEmpty,
        criteria.maxClubs.contains(40),
        criteria.excludeSourceAdmins,
        criteria.excludeFormerMembers,
        criteria.dailyMinElo.contains(1000),
        criteria.dailyMaxElo.isEmpty,
        criteria.dailyMinGamesFinished.contains(20),
        criteria.dailyMinTmGamesFinished.contains(10),
        criteria.dailyMaxTimeoutPercent.contains(5.0),
        criteria.dailyMaxTmTimeoutPercent.contains(0.0),
        criteria.dailyMaxHoursPerMove.contains(12),
        criteria.dailyMinOngoingGames.isEmpty,
        criteria.dailyMaxOngoingGames.contains(60),
        criteria.dailyMinOngoingTeamMatches.isEmpty
      )
    }
  )

  // ==========================================================================
  // Suite: DB CRUD
  // ==========================================================================

  private def suiteDbCrud = suite("DB CRUD")(
    test("RecruitmentCriteria insert and selectId") {
      val criteria = makeCriteria(excludeClubs = List("club-x"))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        loaded     <- RecruitmentCriteria.selectId(criteriaId)
      } yield assertTrue(
        loaded.isDefined,
        loaded.get.excludeClubs == List("club-x")
      )
    },
    test("RecruitmentAlias selectClub") {
      for {
        _    <- seedDb
        cid1 <- RecruitmentCriteria.insert(makeCriteria())
        cid2 <- RecruitmentCriteria.insert(makeCriteria())
        _    <- RecruitmentAlias.insert(RecruitmentAlias(clubId, "cfg1", Instant.now(), cid1))
        _    <- RecruitmentAlias.insert(RecruitmentAlias(clubId, "cfg2", Instant.now(), cid2))
        all  <- RecruitmentAlias.selectClub(clubId)
      } yield assertTrue(all.size == 2)
    },
    test("RecruitmentCriteria insert is insert-only (new ID each time)") {
      val criteria = makeCriteria()
      for {
        _       <- seedDb
        cid1    <- RecruitmentCriteria.insert(criteria)
        cid2    <- RecruitmentCriteria.insert(criteria.copy(dailyMinElo = Some(1500)))
        loaded1 <- RecruitmentCriteria.selectId(cid1)
        loaded2 <- RecruitmentCriteria.selectId(cid2)
      } yield assertTrue(
        cid1 != cid2,
        loaded1.get.dailyMinElo.isEmpty,
        loaded2.get.dailyMinElo.contains(1500)
      )
    },
    test("RecruitmentCriteria TEXT[] array round-trip") {
      val criteria = makeCriteria(
        excludeClubs = List("delta")
      ).copy(nationalityCountries = List("US", "GB", "DE"))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        loaded     <- RecruitmentCriteria.selectId(criteriaId)
      } yield assertTrue(
        loaded.get.excludeClubs == List("delta"),
        loaded.get.nationalityCountries == List("US", "GB", "DE")
      )
    },
    test("RecruitmentRun insert returns generated runId and selectId retrieves") {
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        loaded     <- RecruitmentRun.selectId(runId)
      } yield assertTrue(
        runId > 0,
        loaded.isDefined,
        loaded.get.clubId == clubId,
        loaded.get.criteriaId == criteriaId,
        loaded.get.candidatesFound == 0,
        loaded.get.completedAt.isEmpty
      )
    },
    test("RecruitmentRun update sets completedAt and candidatesFound") {
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        _          <- RecruitmentRun.update(RecruitmentRun(runId, clubId, criteriaId, Times.t0, Some(Times.t1), 5))
        loaded     <- RecruitmentRun.selectId(runId)
      } yield assertTrue(
        loaded.get.completedAt.isDefined,
        loaded.get.candidatesFound == 5
      )
    },
    test("RecruitmentRun selectLatest returns most recent") {
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(makeCriteria())
        _          <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        runId2     <- RecruitmentRun.insert(clubId, criteriaId, Times.t1)
        latest     <- RecruitmentRun.selectLatest(clubId)
      } yield assertTrue(
        latest.isDefined,
        latest.get.runId == runId2
      )
    },
    test("RecruitmentCandidate insert and selectByRun") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        _          <- seedPlayer(pid1)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid1, Times.t0, CandidateOutcome.Rejected, Some("too few games"))
          )
        all <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(all.size == 2)
    },
    test("RecruitmentCandidate selectInvitedByRun filters by outcome") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        _          <- seedPlayer(pid1)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid1, Times.t0, CandidateOutcome.Rejected, Some("reason"))
          )
        invited <- RecruitmentCandidate.selectInvitedByRun(runId)
      } yield assertTrue(
        invited.size == 1,
        invited.head.playerId == pid0
      )
    },
    test("CandidateOutcome enum round-trip for all variants") {
      val enumPids = CandidateOutcome.values.toList.zipWithIndex.map((_, i) => PlayerId.wrap(250L + i))
      for {
        _ <- seedDb
        _ <- ZIO.foreachDiscard(enumPids)(seedPlayer)
        outcomes = CandidateOutcome.values.toList
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        _ <- ZIO.foreachDiscard(outcomes.zip(enumPids)) { (outcome, pid) =>
          RecruitmentCandidate
            .insert(
              RecruitmentCandidate(runId, pid, Times.t0, outcome, Some(s"reason-${pid}"))
            )
        }
        candidates <- RecruitmentCandidate.selectByRun(runId)
        loadedOutcomes = candidates.map(_.outcome).toSet
      } yield assertTrue(
        candidates.size == outcomes.size,
        loadedOutcomes == outcomes.toSet
      )
    },
    test("RecruitmentCandidate selectLatestInvited") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        criteriaId <- seedCriteria(makeCriteria())
        runId1     <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        runId2     <- RecruitmentRun.insert(clubId, criteriaId, Times.t1)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId1, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId2, pid0, Times.t1, CandidateOutcome.Invited, None)
          )
        latest <- RecruitmentCandidate.selectLatestInvited(pid0)
      } yield assertTrue(
        latest.isDefined,
        latest.get.runId == runId2
      )
    },
    test("defaultDaily round-trips through DB via insert/selectId") {
      val criteria = RecruitmentCriteria.defaultDaily
      for {
        _          <- seedDb
        criteriaId <- RecruitmentCriteria.insert(criteria)
        loaded     <- RecruitmentCriteria.selectId(criteriaId)
      } yield assertTrue(
        loaded.isDefined,
        loaded.get.copy(criteriaId = 0) == criteria
      )
    },
    test("ApiFetchFailure insert and selectRecent") {
      val now = Instant.now()
      val failure = ApiFetchFailure(
        url = "https://api.chess.com/pub/player/alice/games/2026/03",
        errorType = "ExternalException",
        errorMessage = Some("HTTP 404"),
        occurredAt = now
      )
      for {
        _        <- seedDb
        _        <- ApiFetchFailure.insert(failure)
        recent   <- ApiFetchFailure.selectRecent(now.minus(Duration.ofMinutes(1)))
        tooEarly <- ApiFetchFailure.selectRecent(now.plus(Duration.ofMinutes(1)))
      } yield assertTrue(
        recent.size == 1,
        recent.head.url == failure.url,
        recent.head.errorType == "ExternalException",
        recent.head.errorMessage.contains("HTTP 404"),
        tooEarly.isEmpty
      )
    }
  )

  // ==========================================================================
  // Suite: Evaluate candidates
  // ==========================================================================

  private def suiteEvaluateCandidates = suite("evaluateCandidates")(
    test("persists Player, PlayerSnapshot, and RecruitmentCandidate") {
      val responses = Map(
        "player/alice" -> apiPlayerJson(200, "alice"),
        "player/bob"   -> apiPlayerJson(201, "bob")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        client     <- fakeChessComClient(responses)
        invited    <- evalCandidates(client, runId, List(Username("alice"), Username("bob")), criteria)
        // Check invited list
        _ = assertTrue(invited.size == 2)
        // Check Player table
        playerA <- Player.selectId(pid0)
        playerB <- Player.selectId(pid1)
        // Check PlayerSnapshot table
        snapA <- PlayerSnapshot.selectIdLatest(pid0)
        snapB <- PlayerSnapshot.selectIdLatest(pid1)
        // Check RecruitmentCandidate table
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        playerA.isDefined,
        playerB.isDefined,
        snapA.isDefined,
        snapA.get.username == Username("alice"),
        snapB.isDefined,
        snapB.get.username == Username("bob"),
        candidates.size == 2,
        candidates.forall(_.outcome == CandidateOutcome.Invited)
      )
    },
    test("respects target limit") {
      val responses = Map(
        "player/alice"   -> apiPlayerJson(200, "alice"),
        "player/bob"     -> apiPlayerJson(201, "bob"),
        "player/charlie" -> apiPlayerJson(202, "charlie")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        client     <- fakeChessComClient(responses)
        invited <- evalCandidates(
          client,
          runId,
          List(Username("alice"), Username("bob"), Username("charlie")),
          criteria,
          target = 2
        )
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        invited.size == 2,
        candidates.size == 2
      )
    },
    test("skips candidate on API failure (no record written)") {
      val responses = Map(
        "player/alice" -> apiPlayerJson(200, "alice")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        client     <- fakeChessComClient(responses, failures = Set("bob"))
        invited    <- evalCandidates(client, runId, List(Username("alice"), Username("bob")), criteria)
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        invited.size == 1,
        invited.head == Username("alice"),
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Invited
      )
    },
    test("mid-pipeline error persists candidate with Error outcome") {
      // Player profile fetches OK (apiPlayer set), but stats returns invalid JSON
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> "NOT VALID JSON"
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username("alice")), criteria)
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Error,
        candidates.head.playerId == pid0,
        candidates.head.rejectionReason.isDefined
      )
    }
  )

  // ==========================================================================
  // Suite: Filter chain
  // ==========================================================================

  /** Helper: run evaluateCandidates with a single candidate and return the outcome. */
  private def evalSingle(
    responses: Map[String, String],
    criteria: RecruitmentCriteria,
    username: String = "alice"
  ): RIO[Transactor, CandidateOutcome] =
    for {
      _          <- seedDb
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
      client     <- fakeChessComClient(responses)
      _          <- evalCandidates(client, runId, List(Username.wrap(username)), criteria)
      cands      <- RecruitmentCandidate.selectByRun(runId)
    } yield cands.head.outcome

  private def suiteFilterChain = suite("filter chain")(
    test("rejects closed account") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", status = "closed"))
      for { outcome <- evalSingle(responses, makeCriteria()) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by minDaysSinceRegistration") {
      // Player joined 5 days ago, config requires 30 days
      val recentJoin = Instant.now().minus(java.time.Duration.ofDays(5)).getEpochSecond
      val responses  = Map("player/alice" -> apiPlayerJson(200, "alice", joined = recentJoin))
      val criteria   = makeCriteria().copy(minDaysSinceRegistration = Some(30))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player meeting minDaysSinceRegistration") {
      // Player joined 60 days ago, config requires 30 days
      val oldJoin   = Instant.now().minus(java.time.Duration.ofDays(60)).getEpochSecond
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", joined = oldJoin))
      val criteria  = makeCriteria().copy(minDaysSinceRegistration = Some(30))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by nationality exclude mode") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "XX"))
      val criteria  = makeCriteria().copy(nationalityExclude = true, nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player not in nationality exclude list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "ZZ"))
      val criteria  = makeCriteria().copy(nationalityExclude = true, nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by nationality include mode when not in list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "ZZ"))
      val criteria  = makeCriteria().copy(nationalityExclude = false, nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player in nationality include list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "YY"))
      val criteria  = makeCriteria().copy(nationalityExclude = false, nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by dailyMinElo") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 800)
      )
      val criteria = makeCriteria().copy(dailyMinElo = Some(1000))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by dailyMaxElo") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 2200)
      )
      val criteria = makeCriteria().copy(dailyMaxElo = Some(2000))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player within Elo range") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 1500)
      )
      val criteria = makeCriteria().copy(dailyMinElo = Some(1000), dailyMaxElo = Some(2000))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by dailyMaxTimeoutPercent") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timeoutPct = 15.0)
      )
      val criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(10.0))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by dailyMinGamesFinished") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 5, losses = 3, draws = 2) // 10 games
      )
      val criteria = makeCriteria().copy(dailyMinGamesFinished = Some(50))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("dailyMinGamesFinished counts team match games from archives") {
      // Archive has 2 TM games + 1 non-TM game = 3 daily games in 90d window
      val now        = Instant.now()
      val recent     = now.minus(java.time.Duration.ofDays(10)).getEpochSecond
      val ym         = java.time.YearMonth.from(java.time.LocalDate.ofInstant(now, java.time.ZoneOffset.UTC))
      val archiveKey = s"player/alice/games/${ym.getYear}/${f"${ym.getMonthValue}%02d"}"
      val games = List(
        archiveGameJson("alice", "bob", endTime = recent, matchUrl = Some("https://api.chess.com/pub/match/111")),
        archiveGameJson(
          "carol",
          "alice",
          whiteResult = "checkmated",
          blackResult = "win",
          endTime = recent,
          matchUrl = Some("https://api.chess.com/pub/match/222")
        ),
        archiveGameJson("alice", "dave", endTime = recent)
      )
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 100, losses = 50, draws = 10),
        archiveKey           -> archiveJson(games)
      )
      // Require 3 games — should pass because TM games are included
      val criteria = makeCriteria().copy(dailyMinGamesFinished = Some(3))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("dailyMinGamesFinished excludes non-daily games from archives") {
      val now        = Instant.now()
      val recent     = now.minus(java.time.Duration.ofDays(10)).getEpochSecond
      val ym         = java.time.YearMonth.from(java.time.LocalDate.ofInstant(now, java.time.ZoneOffset.UTC))
      val archiveKey = s"player/alice/games/${ym.getYear}/${f"${ym.getMonthValue}%02d"}"
      val games = List(
        archiveGameJson("alice", "bob", endTime = recent, timeClass = "daily"),
        archiveGameJson("alice", "carol", endTime = recent, timeClass = "blitz"),
        archiveGameJson("alice", "dave", endTime = recent, timeClass = "rapid")
      )
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 100, losses = 50, draws = 10),
        archiveKey           -> archiveJson(games)
      )
      // Only 1 daily game in archives — require 2, should reject
      val criteria = makeCriteria().copy(dailyMinGamesFinished = Some(2))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("archive fetch failure is recorded in ApiFetchFailure") {
      val now        = Instant.now()
      val ym         = java.time.YearMonth.from(java.time.LocalDate.ofInstant(now, java.time.ZoneOffset.UTC))
      val archiveKey = s"player/alice/games/${ym.getYear}/${f"${ym.getMonthValue}%02d"}"
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 100, losses = 50, draws = 10),
        archiveKey           -> "NOT VALID JSON"
      )
      // dailyMinGamesFinished triggers archive fetch
      val criteria = makeCriteria().copy(dailyMinGamesFinished = Some(1))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username.wrap("alice")), criteria)
        failures   <- ApiFetchFailure.selectRecent(now.minus(Duration.ofMinutes(1)))
        cands      <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        failures.nonEmpty,
        failures.exists(_.errorType == "JsonDecodingException"),
        cands.head.outcome == CandidateOutcome.Error
      )
    },
    test("extractLastDailyTimeout ignores non-daily timeClass games") {
      val now        = Instant.now()
      val recent     = now.minus(java.time.Duration.ofDays(10)).getEpochSecond
      val ym         = java.time.YearMonth.from(java.time.LocalDate.ofInstant(now, java.time.ZoneOffset.UTC))
      val archiveKey = s"player/alice/games/${ym.getYear}/${f"${ym.getMonthValue}%02d"}"
      val games = List(
        // Blitz timeout — should NOT count as lastDailyTimeoutAt
        archiveGameJson(
          "alice",
          "bob",
          whiteResult = "timeout",
          blackResult = "win",
          endTime = recent,
          timeClass = "blitz"
        ),
        // Daily win — no timeout
        archiveGameJson("alice", "carol", endTime = recent, timeClass = "daily")
      )
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timeoutPct = 5.0, wins = 100, losses = 50, draws = 10),
        archiveKey           -> archiveJson(games)
      )
      // timeoutPct > 0 triggers archive fetch; not high enough to reject
      val criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(10.0))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username.wrap("alice")), criteria)
        cache      <- PlayerRecruitmentCache.selectId(pid0)
      } yield assertTrue(
        cache.isDefined,
        cache.get.lastDailyTimeoutAt.isEmpty // blitz timeout should not be stored
      )
    },
    test("rejects by dailyMaxHoursPerMove") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timePerMove = 86400) // 24 hours
      )
      val criteria = makeCriteria().copy(dailyMaxHoursPerMove = Some(12))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player within dailyMaxHoursPerMove") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timePerMove = 36000) // 10 hours
      )
      val criteria = makeCriteria().copy(dailyMaxHoursPerMove = Some(12))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by maxClubs") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/clubs" -> apiPlayerClubsJson(List("club-1", "club-2", "club-3", "club-4", "club-5", "club-6"))
      )
      val criteria = makeCriteria().copy(maxClubs = Some(5))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by excludeClubs") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/clubs" -> apiPlayerClubsJson(List("good-club", "banned-club"))
      )
      val criteria = makeCriteria(excludeClubs = List("banned-club"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects when player has match against target club") {
      val matchId = "https://api.chess.com/pub/match/12345"
      val responses = Map(
        "player/alice"               -> apiPlayerJson(200, "alice"),
        "player/alice/matches"       -> apiPlayerMatchesJson(registeredIds = List(matchId)),
        s"club/$clubUrlName/matches" -> apiClubMatchesJson(registeredIds = List(matchId))
      )
      for { outcome <- evalSingle(responses, makeCriteria()) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts when player matches don't overlap target club") {
      val responses = Map(
        "player/alice"         -> apiPlayerJson(200, "alice"),
        "player/alice/matches" -> apiPlayerMatchesJson(registeredIds = List("https://api.chess.com/pub/match/999")),
        s"club/$clubUrlName/matches" -> apiClubMatchesJson(registeredIds = List("https://api.chess.com/pub/match/888"))
      )
      for { outcome <- evalSingle(responses, makeCriteria()) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by daysSinceLastInvited") {
      val criteria = makeCriteria().copy(daysSinceLastInvited = Some(30))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        // Seed player row for FK constraint
        _ <- seedPlayer(pid0)
        // Create a prior run with alice invited recently
        priorRunId <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(priorRunId, pid0, Instant.now(), CandidateOutcome.Invited, None)
        )
        // Now evaluate alice again
        runId  <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
        client <- fakeChessComClient(Map("player/alice" -> apiPlayerJson(200, "alice")))
        _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Rejected)
    },
    test("gatherClubCandidates excludes existing and evaluated usernames") {
      val responses = Map(
        "club/source-club" -> apiClubJson(sourceClubId.value, "source-club"),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("existing-member", Times.t0.getEpochSecond),
            ("already-evaluated", Times.t0.getEpochSecond),
            ("fresh-candidate", Times.t0.getEpochSecond)
          )
        )
      )

      for {
        client <- fakeChessComClient(responses)
        candidates <- RecruitmentApp.gatherClubCandidates(
          client,
          ClubUrlName("source-club"),
          excludeSourceAdmins = false,
          existingUsernames = Set(Username("existing-member")),
          evaluatedUsernames = Set(Username("already-evaluated"))
        )
      } yield assertTrue(
        candidates == List(Username("fresh-candidate"))
      )
    },
    test("gatherClubCandidates excludes admins when enabled") {
      val responses = Map(
        "club/source-club" -> apiClubJson(sourceClubId.value, "source-club", admins = List("admin-user")),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("admin-user", Times.t0.getEpochSecond),
            ("regular-user", Times.t0.getEpochSecond)
          )
        )
      )

      for {
        client <- fakeChessComClient(responses)
        candidates <- RecruitmentApp.gatherClubCandidates(
          client,
          ClubUrlName("source-club"),
          excludeSourceAdmins = true,
          existingUsernames = Set.empty,
          evaluatedUsernames = Set.empty
        )
      } yield assertTrue(
        candidates.size == 1,
        candidates.head == Username("regular-user")
      )
    },
    test("gatherClubCandidates keeps admins when disabled") {
      val responses = Map(
        "club/source-club" -> apiClubJson(sourceClubId.value, "source-club", admins = List("admin-user")),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("admin-user", Times.t0.getEpochSecond),
            ("regular-user", Times.t0.getEpochSecond)
          )
        )
      )

      for {
        client <- fakeChessComClient(responses)
        candidates <- RecruitmentApp.gatherClubCandidates(
          client,
          ClubUrlName("source-club"),
          excludeSourceAdmins = false,
          existingUsernames = Set.empty,
          evaluatedUsernames = Set.empty
        )
      } yield assertTrue(
        candidates.size == 2,
        candidates.toSet == Set(Username("admin-user"), Username("regular-user"))
      )
    },
    test("cache is populated after evaluation") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val criteria  = makeCriteria()
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cached     <- PlayerRecruitmentCache.selectId(pid0)
      } yield assertTrue(
        cached.isDefined,
        cached.get.clubCount.contains(0),
        cached.get.ongoingGames.contains(0),
        cached.get.dailyElo.contains(1200),
        cached.get.lastDailyTimeoutAt.isEmpty,
        cached.get.lastTmTimeoutAt.isEmpty
      )
    },
    test("former member rejected when excludeFormerMembers = true") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val criteria  = makeCriteria(excludeFormerMembers = true)
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        // Seed alice as a former member of the club (player row needed for FK)
        _ <- SqlZioTypes.connectZIO {
          sql"""INSERT INTO player (player_id, joined) VALUES ($pid0, ${Times.t0})
                ON CONFLICT (player_id) DO NOTHING""".update.run()
        }
        _      <- ClubMember.insert(ClubMember(clubId, pid0, Times.t0, Some(Times.t1)))
        runId  <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
        client <- fakeChessComClient(responses)
        _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Rejected)
    },
    test("former member accepted when excludeFormerMembers = false") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val criteria  = makeCriteria(excludeFormerMembers = false)
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        // Seed alice as a former member of the club
        _ <- SqlZioTypes.connectZIO {
          sql"""INSERT INTO player (player_id, joined) VALUES ($pid0, ${Times.t0})
                ON CONFLICT (player_id) DO NOTHING""".update.run()
        }
        _      <- ClubMember.insert(ClubMember(clubId, pid0, Times.t0, Some(Times.t1)))
        runId  <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
        client <- fakeChessComClient(responses)
        _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Invited)
    }
  )

  // ==========================================================================
  // Suite: Blacklist
  // ==========================================================================

  private def suiteBlacklist = suite("blacklist")(
    test("blacklisted player is rejected during evaluation") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val criteria  = makeCriteria()
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        // Blacklist alice (indefinite)
        _ <- RecruitmentBlacklist.insert(
          RecruitmentBlacklist(clubId, pid0, Times.t0, expiresAt = None, reason = Some("banned"))
        )
        runId  <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
        client <- fakeChessComClient(responses)
        _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        cands.size == 1,
        cands.head.outcome == CandidateOutcome.Rejected
      )
    },
    test("expired blacklist entry does not reject the player") {
      val now       = Instant.now()
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val criteria  = makeCriteria()
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        // Blacklist alice with an already-expired entry
        _ <- RecruitmentBlacklist.insert(
          RecruitmentBlacklist(
            clubId,
            pid0,
            Times.t0,
            expiresAt = Some(now.minus(java.time.Duration.ofDays(1))),
            reason = Some("temp ban")
          )
        )
        runId  <- RecruitmentRun.insert(clubId, criteriaId, now)
        client <- fakeChessComClient(responses)
        _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        cands.size == 1,
        cands.head.outcome == CandidateOutcome.Invited
      )
    }
  )

  // ==========================================================================
  // Suite: BlacklistApp
  // ==========================================================================

  private val blacklistClubId      = ClubId(700)
  private val blacklistClubUrlName = ClubUrlName("blacklist-club")

  private def suiteBlacklistApp = suite("BlacklistApp")(
    test("inserts blacklist entry with reason and expiresAt") {
      val futureInstant = Times.t3
      val responses = Map(
        s"club/$blacklistClubUrlName" -> apiClubJson(700, blacklistClubUrlName.value, Nil),
        "player/target-player"        -> apiPlayerJson(203, "target-player")
      )
      for {
        _      <- seedDb
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        _ <- BlacklistApp.addToBlacklist(
          blacklistClubUrlName,
          Username("target-player"),
          Some("toxic"),
          Some(futureInstant)
        )
          .provideEnvironment(zio.ZEnvironment(client, xa))
        entries <- RecruitmentBlacklist.selectByClub(blacklistClubId)
      } yield assertTrue(
        entries.size == 1,
        entries.head.playerId == pid3,
        entries.head.reason.contains("toxic"),
        entries.head.expiresAt.contains(futureInstant)
      )
    },
    test("inserts blacklist entry without optional fields") {
      val responses = Map(
        s"club/$blacklistClubUrlName" -> apiClubJson(700, blacklistClubUrlName.value, Nil),
        "player/target-player"        -> apiPlayerJson(203, "target-player")
      )
      for {
        _      <- seedDb
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        _ <- BlacklistApp.addToBlacklist(blacklistClubUrlName, Username("target-player"), None, None)
          .provideEnvironment(zio.ZEnvironment(client, xa))
        entries <- RecruitmentBlacklist.selectByClub(blacklistClubId)
      } yield assertTrue(
        entries.size == 1,
        entries.head.reason.isEmpty,
        entries.head.expiresAt.isEmpty
      )
    },
    test("upserts club before inserting blacklist entry") {
      val freshClubId      = ClubId(701)
      val freshClubUrlName = ClubUrlName("fresh-club")
      val responses = Map(
        s"club/$freshClubUrlName" -> apiClubJson(701, freshClubUrlName.value, Nil),
        "player/target-player"    -> apiPlayerJson(203, "target-player")
      )
      for {
        _      <- seedDb
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        before <- Club.selectId(freshClubId)
        _ <- BlacklistApp.addToBlacklist(freshClubUrlName, Username("target-player"), None, None)
          .provideEnvironment(zio.ZEnvironment(client, xa))
        after <- Club.selectId(freshClubId)
      } yield assertTrue(
        before.isEmpty,
        after.isDefined,
        after.get.urlName == freshClubUrlName
      )
    }
  )

  // ==========================================================================
  // Suite: Cache-aware filters
  // ==========================================================================

  /** Helper: run evalSingle but seed a cache row (and its player FK) before evaluation. */
  private def evalSingleWithCache(
    responses: Map[String, String],
    criteria: RecruitmentCriteria,
    cache: PlayerRecruitmentCache,
    username: String = "alice"
  ): RIO[Transactor, CandidateOutcome] =
    for {
      _ <- seedDb
      // Seed player row for FK constraint, then seed cache
      _ <- SqlZioTypes.connectZIO {
        sql"""INSERT INTO player (player_id, joined)
              VALUES (${cache.playerId}, ${Times.t0})
              ON CONFLICT (player_id) DO NOTHING""".update.run()
      }
      _          <- PlayerRecruitmentCache.upsert(cache)
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, Instant.now())
      client     <- fakeChessComClient(responses)
      _          <- evalCandidates(client, runId, List(Username.wrap(username)), criteria)
      cands      <- RecruitmentCandidate.selectByRun(runId)
    } yield cands.head.outcome

  private def suiteCacheFilters = suite("cache-aware filters")(
    test("zero-tolerance daily timeout rejects from cache") {
      val now = Instant.now()
      val staleCache = PlayerRecruitmentCache(
        playerId = pid0,
        fetchedAt = now.minus(java.time.Duration.ofDays(30)), // very old cache
        dailyElo = Some(1500),
        dailyTimeoutPct = Some(0.0),
        dailyGamesFinished = Some(200),
        clubCount = Some(5),
        ongoingGames = Some(3),
        ongoingTeamMatches = Some(2),
        tmGamesFinished90d = Some(10),
        tmTimeoutPct90d = Some(0.0),
        lastDailyTimeoutAt = Some(now.minus(java.time.Duration.ofDays(100))), // had a timeout once
        lastTmTimeoutAt = None
      )
      val criteria  = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0))
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))

      for {
        outcome <- evalSingleWithCache(responses, criteria, staleCache)
      } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("maxClubs cache rejection at 48h old cache") {
      val now = Instant.now()
      val cache48h = PlayerRecruitmentCache(
        playerId = pid0,
        fetchedAt = now.minus(java.time.Duration.ofHours(48)),
        dailyElo = Some(1500),
        dailyTimeoutPct = Some(0.0),
        dailyGamesFinished = Some(200),
        clubCount = Some(120), // way over limit
        ongoingGames = Some(3),
        ongoingTeamMatches = Some(2),
        tmGamesFinished90d = Some(10),
        tmTimeoutPct90d = Some(0.0),
        lastDailyTimeoutAt = None,
        lastTmTimeoutAt = None
      )
      val criteria  = makeCriteria().copy(maxClubs = Some(50))
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))

      for {
        outcome <- evalSingleWithCache(responses, criteria, cache48h)
      } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("stale cache falls through to API checks") {
      val now = Instant.now()
      val staleCache = PlayerRecruitmentCache(
        playerId = pid0,
        fetchedAt = now.minus(java.time.Duration.ofHours(100)),
        dailyElo = Some(500),
        dailyTimeoutPct = Some(50.0),
        dailyGamesFinished = Some(5),
        clubCount = Some(120),
        ongoingGames = Some(0),
        ongoingTeamMatches = Some(0),
        tmGamesFinished90d = Some(0),
        tmTimeoutPct90d = None,
        lastDailyTimeoutAt = None,
        lastTmTimeoutAt = None
      )
      val criteria = makeCriteria().copy(
        maxClubs = Some(50),
        dailyMinElo = Some(1000),
        dailyMaxTimeoutPercent = Some(10.0)
      )
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 1500, timeoutPct = 2.0),
        "player/alice/clubs" -> apiPlayerClubsJson(List("club-a", "club-b"))
      )

      for {
        outcome <- evalSingleWithCache(responses, criteria, staleCache)
      } yield assertTrue(outcome == CandidateOutcome.Invited)
    }
  )

  // ==========================================================================
  // Suite: Full workflow (end-to-end)
  // ==========================================================================

  private def suiteFullWorkflow = suite("full workflow")(
    test("recruit end-to-end") {
      val responses = Map(
        s"club/$clubUrlName" -> apiClubJson(clubId.value, clubUrlName.value),
        s"club/$clubUrlName/members" -> apiClubMembersJson(
          List(
            ("existing", Times.t0.getEpochSecond)
          )
        ),
        "club/source-club" -> apiClubJson(sourceClubId.value, "source-club"),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("existing", Times.t0.getEpochSecond),
            ("candidate-a", Times.t0.getEpochSecond),
            ("candidate-b", Times.t0.getEpochSecond)
          )
        ),
        "player/existing"    -> apiPlayerJson(199, "existing"),
        "player/candidate-a" -> apiPlayerJson(200, "candidate-a"),
        "player/candidate-b" -> apiPlayerJson(201, "candidate-b")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        result <- RecruitmentApp.recruit(clubUrlName, "default", sourceClubs = List(ClubUrlName("source-club")))
          .provideEnvironment(zio.ZEnvironment(client, xa))
        // Verify run record
        run <- RecruitmentRun.selectId(result.runId)
        // Verify candidates
        invited <- RecruitmentCandidate.selectInvitedByRun(result.runId)
        // Verify Player/PlayerSnapshot persistence
        playerA <- Player.selectId(pid0)
        playerB <- Player.selectId(pid1)
      } yield assertTrue(
        run.isDefined,
        run.get.completedAt.isDefined,
        run.get.candidatesFound == 2,
        invited.size == 2,
        playerA.isDefined,
        playerB.isDefined
      )
    }
  )

  // ==========================================================================
  // Suite: Explore mode
  // ==========================================================================

  private val discoverableClubId      = ClubId(701)
  private val discoverableClubUrlName = ClubUrlName("discoverable-club")

  private def suiteExploreMode = suite("explore mode")(
    test("isGrim pure logic") {
      import RecruitmentApp.{SourceState, isGrim}
      assertTrue(
        !isGrim(SourceState(Nil, 49, 49, 49)), // below threshold
        isGrim(SourceState(Nil, 10, 10, 50)),  // consecutive threshold hit
        !isGrim(SourceState(Nil, 40, 39, 5)),  // high ratio but low consecutive — not grim
        !isGrim(SourceState(Nil, 0, 0, 0))     // fresh source
      )
    },
    test("explore=false does not explore beyond source clubs") {
      val responses = Map(
        s"club/$clubUrlName"             -> apiClubJson(clubId.value, clubUrlName.value),
        s"club/$clubUrlName/members"     -> apiClubMembersJson(Nil),
        s"club/$discoverableClubUrlName" -> apiClubJson(discoverableClubId.value, discoverableClubUrlName.value),
        s"club/$discoverableClubUrlName/members" -> apiClubMembersJson(
          List(("explorer", Times.t0.getEpochSecond))
        ),
        "player/explorer" -> apiPlayerJson(210, "explorer")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- Club.upsert(Club(discoverableClubId, Times.t0, discoverableClubUrlName))
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        result <- RecruitmentApp.recruit(clubUrlName, "default", sourceClubs = Nil, explore = false)
          .provideEnvironment(zio.ZEnvironment(client, xa))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.isEmpty
      )
    },
    test("explore=true discovers candidates from DB clubs") {
      val responses = Map(
        s"club/$clubUrlName"             -> apiClubJson(clubId.value, clubUrlName.value),
        s"club/$clubUrlName/members"     -> apiClubMembersJson(Nil),
        s"club/$discoverableClubUrlName" -> apiClubJson(discoverableClubId.value, discoverableClubUrlName.value),
        s"club/$discoverableClubUrlName/members" -> apiClubMembersJson(
          List(("explorer", Times.t0.getEpochSecond))
        ),
        "player/explorer" -> apiPlayerJson(210, "explorer")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- Club.upsert(Club(discoverableClubId, Times.t0, discoverableClubUrlName))
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        result <- RecruitmentApp.recruit(clubUrlName, "default", sourceClubs = Nil, explore = true)
          .provideEnvironment(zio.ZEnvironment(client, xa))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Invited,
        candidates.head.playerId == PlayerId(210)
      )
    },
    test("explore=true discovers candidates from match opponents") {
      val clubMatchesWithOpponent =
        s"""{"finished": [{"name": "match", "@id": "https://api.chess.com/pub/match/99", "opponent": "https://api.chess.com/pub/club/opponent-club", "time_class": "daily", "start_time": ${Times.t0.getEpochSecond}, "result": "win"}], "in_progress": [], "registered": []}"""
      val opponentClubId = ClubId(702)
      val responses = Map(
        s"club/$clubUrlName"         -> apiClubJson(clubId.value, clubUrlName.value),
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        s"club/$clubUrlName/matches" -> clubMatchesWithOpponent,
        "club/opponent-club"         -> apiClubJson(opponentClubId.value, "opponent-club"),
        "club/opponent-club/members" -> apiClubMembersJson(
          List(("opp-player", Times.t0.getEpochSecond))
        ),
        "player/opp-player" -> apiPlayerJson(211, "opp-player")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        result <- RecruitmentApp.recruit(clubUrlName, "default", sourceClubs = Nil, explore = true)
          .provideEnvironment(zio.ZEnvironment(client, xa))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Invited,
        candidates.head.playerId == PlayerId(211)
      )
    },
    test("explore=true respects invite cap across sources") {
      val source1 = ClubUrlName("source-1")
      val source2 = ClubUrlName("source-2")
      val responses = Map(
        s"club/$clubUrlName"         -> apiClubJson(clubId.value, clubUrlName.value),
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        s"club/$source1"             -> apiClubJson(ClubId(801).value, source1.value),
        s"club/$source1/members" -> apiClubMembersJson(
          List(("cap-a", Times.t0.getEpochSecond), ("cap-b", Times.t0.getEpochSecond))
        ),
        s"club/$source2" -> apiClubJson(ClubId(802).value, source2.value),
        s"club/$source2/members" -> apiClubMembersJson(
          List(("cap-c", Times.t0.getEpochSecond), ("cap-d", Times.t0.getEpochSecond))
        ),
        "player/cap-a" -> apiPlayerJson(220, "cap-a"),
        "player/cap-b" -> apiPlayerJson(221, "cap-b"),
        "player/cap-c" -> apiPlayerJson(222, "cap-c"),
        "player/cap-d" -> apiPlayerJson(223, "cap-d")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        result <- RecruitmentApp.recruit(clubUrlName, "default", target = Some(3), sourceClubs = List(source1, source2))
          .provideEnvironment(zio.ZEnvironment(client, xa))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
        invited  = candidates.filter(_.outcome == CandidateOutcome.Invited)
        deferred = candidates.filter(_.outcome == CandidateOutcome.Deferred)
      } yield assertTrue(
        invited.size == 3,
        result.candidatesFound == 3,
        deferred.size + invited.size >= 3,
        deferred.size + invited.size <= 4
      )
    },
    test("interrupted recruit persists partial results") {
      val intSource      = ClubUrlName("int-source")
      val candidateNames = (0 to 4).map(i => s"int-cand-$i").toList
      val responses = Map(
        s"club/$clubUrlName"         -> apiClubJson(clubId.value, clubUrlName.value),
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        s"club/$intSource"           -> apiClubJson(intSourceClubId.value, intSource.value),
        s"club/$intSource/members" -> apiClubMembersJson(
          candidateNames.map(n => (n, Times.t0.getEpochSecond))
        )
      ) ++ candidateNames.zipWithIndex.map { (name, i) =>
        s"player/$name" -> apiPlayerJson(300 + i, name)
      }.toMap
      val criteria = makeCriteria()

      for {
        _       <- seedDb
        _       <- seedCriteria(criteria)
        reached <- Promise.make[Nothing, Unit]
        gate    <- Promise.make[Nothing, Unit]
        client  <- fakeChessComClientWithBlock(responses, blockAfterN = 4, reached, gate)
        xa      <- ZIO.service[Transactor]
        fiber <- RecruitmentApp.recruit(clubUrlName, "default", sourceClubs = List(intSource))
          .provideEnvironment(zio.ZEnvironment(client, xa))
          .fork
        _      <- reached.await
        _      <- fiber.interrupt
        latest <- RecruitmentRun.selectLatest(clubId)
        runId = latest.get.runId
        cands <- RecruitmentCandidate.selectByRun(runId)
        invited = cands.filter(_.outcome == CandidateOutcome.Invited)
      } yield assertTrue(
        latest.isDefined,
        latest.get.completedAt.isDefined,
        invited.nonEmpty
      )
    } @@ TestAspect.withLiveClock,
    test("excess invited candidates are reclassified as Deferred") {
      val source = ClubUrlName("defer-source")
      // 6 candidates, target=2 → should get exactly 2 Invited, rest Deferred or Rejected
      val candidateNames = (0 to 5).map(i => s"defer-cand-$i").toList
      val responses = Map(
        s"club/$clubUrlName"         -> apiClubJson(clubId.value, clubUrlName.value),
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        s"club/$source"              -> apiClubJson(ClubId(901).value, source.value),
        s"club/$source/members" -> apiClubMembersJson(
          candidateNames.map(n => (n, Times.t0.getEpochSecond))
        )
      ) ++ candidateNames.zipWithIndex.map { (name, i) =>
        s"player/$name" -> apiPlayerJson(400 + i, name)
      }.toMap
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        result <- RecruitmentApp.recruit(clubUrlName, "default", target = Some(2), sourceClubs = List(source))
          .provideEnvironment(zio.ZEnvironment(client, xa))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
        invited  = candidates.filter(_.outcome == CandidateOutcome.Invited)
        deferred = candidates.filter(_.outcome == CandidateOutcome.Deferred)
      } yield assertTrue(
        invited.size == 2,
        result.candidatesFound == 2,
        // Some candidates may be deferred (those that passed filters but exceeded target)
        invited.size + deferred.size >= 2
      )
    },
    test("deferred candidates from prior run are prioritised in next run") {
      val source = ClubUrlName("prio-source")
      // Seed a prior run with a Deferred candidate, then run again
      val responses = Map(
        s"club/$clubUrlName"         -> apiClubJson(clubId.value, clubUrlName.value),
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        s"club/$source"              -> apiClubJson(ClubId(902).value, source.value),
        s"club/$source/members" -> apiClubMembersJson(
          List(("prio-new", Times.t0.getEpochSecond))
        ),
        "player/prio-deferred" -> apiPlayerJson(500, "prio-deferred"),
        "player/prio-new"      -> apiPlayerJson(501, "prio-new")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        client     <- fakeChessComClient(responses)
        xa         <- ZIO.service[Transactor]

        // Seed prior run with a Deferred candidate (need Player + Snapshot)
        _ <- seedPlayer(PlayerId(500))
        _ <- PlayerSnapshot.insert(
          PlayerSnapshot(
            PlayerId(500),
            Times.t0,
            Username.wrap("prio-deferred"),
            ccas.api.misc.enums.PlayerStatusCategory.Active,
            None
          )
        )
        priorRunId <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        _          <- RecruitmentRun.update(RecruitmentRun(priorRunId, clubId, criteriaId, Times.t0, Some(Times.t1), 0))
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(priorRunId, PlayerId(500), Times.t0, CandidateOutcome.Deferred, None)
        )

        // Verify selectDeferredByClub finds it
        deferredBefore <- RecruitmentCandidate.selectDeferredByClub(clubId)

        // Run recruitment — deferred candidate should be picked up as priority
        result <- RecruitmentApp.recruit(clubUrlName, "default", target = Some(10), sourceClubs = List(source))
          .provideEnvironment(zio.ZEnvironment(client, xa))

        // The deferred candidate should now have an Invited outcome in the new run
        newCandidates <- RecruitmentCandidate.selectByRun(result.runId)
        newInvited     = newCandidates.filter(_.outcome == CandidateOutcome.Invited)
        newInvitedPids = newInvited.map(_.playerId).toSet

        // After the new run, selectDeferredByClub should no longer return the candidate
        deferredAfter <- RecruitmentCandidate.selectDeferredByClub(clubId)
      } yield assertTrue(
        deferredBefore.size == 1,
        deferredBefore.head.playerId == PlayerId(500),
        newInvitedPids.contains(PlayerId(500)),
        deferredAfter.isEmpty
      )
    },
    test("selectDeferredByClub excludes candidates resolved in later runs") {
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        _          <- seedPlayer(PlayerId(600))
        _ <- PlayerSnapshot.insert(
          PlayerSnapshot(
            PlayerId(600),
            Times.t0,
            Username.wrap("resolved-player"),
            ccas.api.misc.enums.PlayerStatusCategory.Active,
            None
          )
        )

        // Run 1: candidate is Deferred
        runId1 <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        _      <- RecruitmentRun.update(RecruitmentRun(runId1, clubId, criteriaId, Times.t0, Some(Times.t1), 0))
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(runId1, PlayerId(600), Times.t0, CandidateOutcome.Deferred, None)
        )

        // Should find the deferred candidate
        deferredBefore <- RecruitmentCandidate.selectDeferredByClub(clubId)

        // Run 2: same candidate is Invited (later timestamp)
        runId2 <- RecruitmentRun.insert(clubId, criteriaId, Times.t2)
        _      <- RecruitmentRun.update(RecruitmentRun(runId2, clubId, criteriaId, Times.t2, Some(Times.t3), 1))
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(runId2, PlayerId(600), Times.t2, CandidateOutcome.Invited, None)
        )

        // Should no longer find the deferred candidate
        deferredAfter <- RecruitmentCandidate.selectDeferredByClub(clubId)
      } yield assertTrue(
        deferredBefore.size == 1,
        deferredAfter.isEmpty
      )
    }
  )

  // ==========================================================================
  // Suite: Report mode
  // ==========================================================================

  private def suiteReport = suite("report mode")(
    test("showReport displays invited candidates") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        _          <- seedPlayer(pid1)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        _          <- RecruitmentRun.update(RecruitmentRun(runId, clubId, criteriaId, Times.t0, Some(Times.t1), 2))
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid1, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentApp.showReport(clubUrlName, Some(runId.toString))
      } yield assertTrue(true)
    },
    test("showReport with latest run") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, Times.t0)
        _          <- RecruitmentRun.update(RecruitmentRun(runId, clubId, criteriaId, Times.t0, Some(Times.t1), 1))
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentApp.showReport(clubUrlName, None)
      } yield assertTrue(true)
    }
  )
}
