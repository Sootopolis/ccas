package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Ref, Scope, Semaphore, ZIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubUrlName, PlayerId, Username}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.{DataSourceLayer, SqlZioTypes}
import ccas.utils.sql.DbCodecs.given

object TestRecruitmentApp extends ZIOSpecDefault {

  // --- Timestamps ---

  private object T {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(30))
    val t3: Instant = t0.plus(Duration.ofDays(60))
  }

  // --- IDs ---

  private val clubId      = ClubId(500)
  private val clubUrlName = ClubUrlName("test-club")
  private val club        = Club(clubId, T.t0, clubUrlName)

  private val sourceClubId = ClubId(600)

  private val pid0 = PlayerId(200)
  private val pid1 = PlayerId(201)
  private val pid2 = PlayerId(202)
  private val pid3 = PlayerId(203)

  // --- Helpers ---

  private def apiPlayerJson(
      playerId: Long,
      username: String,
      status: String = "basic",
      joined: Long = T.t0.getEpochSecond,
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
       |  "created": ${T.t0.getEpochSecond},
       |  "last_activity": ${T.t1.getEpochSecond},
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

  private def apiClubMembersJson(members: List[(String, Long)]): String = {
    val memberJsons = members.map { (username, joined) =>
      s"""{"username": "$username", "joined": $joined}"""
    }
    s"""{"weekly": [${memberJsons.mkString(",")}], "monthly": [], "all_time": []}"""
  }

  private def fakeChessComClient(
      responses: Map[String, String],
      failures: Set[String] = Set.empty
    ): ZIO[Any, Nothing, ChessComClient] =
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
          if failures.contains(username) then Response(status = Status.NotFound)
          else responses.get(s"player/$username").fold(Response(status = Status.NotFound))(Response.json(_))
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
          )(implicit trace: zio.Trace
          ): ZIO[Scope, Throwable, Response] =
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

  private def seedDb: ZIO[Transactor, Throwable, Unit] =
    for {
      // Clean up test data
      _ <- RecruitmentCandidate.deleteAll
      _ <- RecruitmentRun.deleteAll
      _ <- RecruitmentBlacklist.deleteAll
      _ <- RecruitmentConfig.deleteAll
      _ <- PlayerRecruitmentCache.deleteAll
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_member WHERE club_id = $clubId".update.run())
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_member WHERE club_id = $sourceClubId".update.run())
      _ <- ZIO.foreachDiscard(List(blacklistClubId, ClubId(701))) { cid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM club WHERE club_id = $cid".update.run())
      }
      _ <- ZIO.foreachDiscard(List(PlayerId(199), pid0, pid1, pid2, pid3)) { pid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run()) *>
          SqlZioTypes.connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- Club.upsert(club)
    } yield ()

  private def makeConfig(
      excludeClubs: List[String] = Nil,
      onExhaustion: ExhaustionBehavior = ExhaustionBehavior.Stop,
      excludeFormerMembers: Boolean = false
    ): RecruitmentConfig =
    RecruitmentConfig(
      clubId = clubId,
      configName = "default",
      excludeClubs = excludeClubs,
      onExhaustion = onExhaustion,
      nationalityMode = None,
      nationalityCountries = Nil,
      maxClubs = None,
      dailyMaxTimeoutPercent = None,
      dailyMaxTmTimeoutPercent = None,
      dailyMinOngoingGames = None,
      dailyMaxOngoingGames = None,
      dailyMinOngoingTeamMatches = None,
      dailyMinElo = None,
      dailyMaxElo = None,
      dailyMinGamesFinished = None,
      dailyMinTmGamesFinished = None,
      minDaysSinceRegistration = None,
      daysSinceLastInvited = None,
      dailyMaxHoursPerMove = None,
      excludeSourceAdmins = true,
      excludeFormerMembers = excludeFormerMembers
    )

  // --- Spec ---

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentApp")(
    suiteConfigHelpers,
    suiteDbCrud,
    suiteGatherCandidates,
    suiteEvaluateCandidates,
    suiteFilterChain,
    suiteBlacklist,
    suiteBlacklistApp,
    suiteCacheFilters,
    suiteFullWorkflow,
    suiteReport
  ).provideShared(
    DataSourceLayer.liveFromPrefix(schema = Some("test_recruitment_app"), onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  // ==========================================================================
  // Suite: Config helper methods (pure)
  // ==========================================================================

  private def suiteConfigHelpers = suite("config helpers")(
    test("excludeClubNames wraps strings to ClubUrlName") {
      val config = makeConfig(excludeClubs = List("club-x"))
      assertTrue(
        config.excludeClubNames == List(ClubUrlName("club-x"))
      )
    },
    test("onExhaustion stores Stop enum value") {
      val config = makeConfig(onExhaustion = ExhaustionBehavior.Stop)
      assertTrue(config.onExhaustion == ExhaustionBehavior.Stop)
    },
    test("onExhaustion stores Explore enum value") {
      val config = makeConfig(onExhaustion = ExhaustionBehavior.Explore)
      assertTrue(config.onExhaustion == ExhaustionBehavior.Explore)
    },
    test("defaultDaily returns expected field values") {
      val config = RecruitmentConfig.defaultDaily(clubId)
      assertTrue(
        config.clubId == clubId,
        config.configName == "daily",
        config.excludeClubs.isEmpty,
        config.onExhaustion == ExhaustionBehavior.Explore,
        config.nationalityMode.isEmpty,
        config.nationalityCountries.isEmpty,
        config.maxClubs.contains(40),
        config.dailyMaxTimeoutPercent.contains(5.0),
        config.dailyMaxTmTimeoutPercent.contains(0.0),
        config.dailyMinOngoingGames.isEmpty,
        config.dailyMaxOngoingGames.contains(60),
        config.dailyMinOngoingTeamMatches.isEmpty,
        config.dailyMinElo.contains(1000),
        config.dailyMaxElo.isEmpty,
        config.dailyMinGamesFinished.contains(20),
        config.dailyMinTmGamesFinished.contains(10),
        config.minDaysSinceRegistration.contains(90),
        config.daysSinceLastInvited.contains(180),
        config.dailyMaxHoursPerMove.contains(12),
        config.excludeSourceAdmins,
        config.excludeFormerMembers
      )
    }
  )

  // ==========================================================================
  // Suite: DB CRUD
  // ==========================================================================

  private def suiteDbCrud = suite("DB CRUD")(
    test("RecruitmentConfig upsert and select") {
      val config = makeConfig(excludeClubs = List("club-x"))
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        loaded <- RecruitmentConfig.select(clubId, "default")
      } yield assertTrue(
        loaded.isDefined,
        loaded.get.excludeClubs == List("club-x"),
        loaded.get.onExhaustion == ExhaustionBehavior.Stop
      )
    },
    test("RecruitmentConfig selectClub") {
      val config1 = makeConfig().copy(configName = "cfg1")
      val config2 = makeConfig().copy(configName = "cfg2")
      for {
        _   <- seedDb
        _   <- RecruitmentConfig.upsert(config1)
        _   <- RecruitmentConfig.upsert(config2)
        all <- RecruitmentConfig.selectClub(clubId)
      } yield assertTrue(all.size == 2)
    },
    test("RecruitmentConfig upsert updates existing") {
      val config  = makeConfig()
      val updated = config.copy(dailyMinElo = Some(1500))
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        _      <- RecruitmentConfig.upsert(updated)
        loaded <- RecruitmentConfig.select(clubId, "default")
      } yield assertTrue(loaded.get.dailyMinElo.contains(1500))
    },
    test("RecruitmentConfig TEXT[] array round-trip") {
      val config = makeConfig(
        excludeClubs = List("delta")
      ).copy(nationalityCountries = List("US", "GB", "DE"))
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        loaded <- RecruitmentConfig.select(clubId, "default")
      } yield assertTrue(
        loaded.get.excludeClubs == List("delta"),
        loaded.get.nationalityCountries == List("US", "GB", "DE")
      )
    },
    test("RecruitmentRun insert returns generated runId and selectId retrieves") {
      for {
        _      <- seedDb
        runId  <- RecruitmentRun.insert(clubId, "default", T.t0)
        loaded <- RecruitmentRun.selectId(runId)
      } yield assertTrue(
        runId > 0,
        loaded.isDefined,
        loaded.get.clubId == clubId,
        loaded.get.configName == "default",
        loaded.get.candidatesFound == 0,
        loaded.get.completedAt.isEmpty
      )
    },
    test("RecruitmentRun update sets completedAt and candidatesFound") {
      for {
        _      <- seedDb
        runId  <- RecruitmentRun.insert(clubId, "default", T.t0)
        _      <- RecruitmentRun.update(RecruitmentRun(runId, clubId, "default", T.t0, Some(T.t1), 5))
        loaded <- RecruitmentRun.selectId(runId)
      } yield assertTrue(
        loaded.get.completedAt.isDefined,
        loaded.get.candidatesFound == 5
      )
    },
    test("RecruitmentRun selectLatest returns most recent") {
      for {
        _      <- seedDb
        _      <- RecruitmentRun.insert(clubId, "default", T.t0)
        runId2 <- RecruitmentRun.insert(clubId, "default", T.t1)
        latest <- RecruitmentRun.selectLatest(clubId)
      } yield assertTrue(
        latest.isDefined,
        latest.get.runId == runId2
      )
    },
    test("RecruitmentCandidate insert and selectByRun") {
      for {
        _     <- seedDb
        runId <- RecruitmentRun.insert(clubId, "default", T.t0)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, Username("alice"), T.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, Username("bob"), T.t0, CandidateOutcome.Rejected, Some("too few games"))
          )
        all <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(all.size == 2)
    },
    test("RecruitmentCandidate selectInvitedByRun filters by outcome") {
      for {
        _     <- seedDb
        runId <- RecruitmentRun.insert(clubId, "default", T.t0)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, Username("alice"), T.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, Username("bob"), T.t0, CandidateOutcome.Rejected, Some("reason"))
          )
        invited <- RecruitmentCandidate.selectInvitedByRun(runId)
      } yield assertTrue(
        invited.size == 1,
        invited.head.username == Username("alice")
      )
    },
    test("CandidateOutcome enum round-trip for all variants") {
      for {
        _     <- seedDb
        runId <- RecruitmentRun.insert(clubId, "default", T.t0)
        outcomes = CandidateOutcome.values.toList
        _ <- ZIO.foreachDiscard(outcomes.zipWithIndex) { (outcome, i) =>
          RecruitmentCandidate
            .insert(
              RecruitmentCandidate(runId, Username.wrap(s"user-$i"), T.t0, outcome, Some(s"reason-$i"))
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
        _      <- seedDb
        runId1 <- RecruitmentRun.insert(clubId, "default", T.t0)
        runId2 <- RecruitmentRun.insert(clubId, "default", T.t1)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId1, Username("alice"), T.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId2, Username("alice"), T.t1, CandidateOutcome.Invited, None)
          )
        latest <- RecruitmentCandidate.selectLatestInvited(Username("alice"))
      } yield assertTrue(
        latest.isDefined,
        latest.get.runId == runId2
      )
    },
    test("defaultDaily round-trips through DB via upsert/select") {
      val config = RecruitmentConfig.defaultDaily(clubId)
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        loaded <- RecruitmentConfig.select(clubId, "daily")
      } yield assertTrue(loaded.contains(config))
    }
  )

  // ==========================================================================
  // Suite: Gather candidates
  // ==========================================================================

  private def suiteGatherCandidates = suite("gatherCandidates")(
    test("deduplicates and filters out existing members") {
      val responses = Map(
        s"club/$clubUrlName/members" -> apiClubMembersJson(
          List(
            ("existing-member", T.t0.getEpochSecond)
          )
        ),
        "club/source-club" -> apiClubJson(sourceClubId, "source-club"),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("existing-member", T.t0.getEpochSecond),
            ("candidate-a", T.t0.getEpochSecond),
            ("candidate-b", T.t0.getEpochSecond)
          )
        )
      )
      val config = makeConfig()

      for {
        _          <- seedDb
        client     <- fakeChessComClient(responses)
        candidates <- RecruitmentApp.gatherCandidates(client, clubUrlName, config, List(ClubUrlName("source-club")))
      } yield assertTrue(
        candidates.size == 2,
        !candidates.contains(Username("existing-member")),
        candidates.toSet == Set(Username("candidate-a"), Username("candidate-b"))
      )
    },
    test("deduplicates across multiple source clubs") {
      val responses = Map(
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        "club/source-a" -> apiClubJson(601, "source-a"),
        "club/source-a/members" -> apiClubMembersJson(
          List(
            ("shared-player", T.t0.getEpochSecond),
            ("unique-a", T.t0.getEpochSecond)
          )
        ),
        "club/source-b" -> apiClubJson(602, "source-b"),
        "club/source-b/members" -> apiClubMembersJson(
          List(
            ("shared-player", T.t0.getEpochSecond),
            ("unique-b", T.t0.getEpochSecond)
          )
        )
      )
      val config = makeConfig()

      for {
        _          <- seedDb
        client     <- fakeChessComClient(responses)
        candidates <- RecruitmentApp.gatherCandidates(client, clubUrlName, config, List(ClubUrlName("source-a"), ClubUrlName("source-b")))
      } yield assertTrue(
        candidates.size == 3,
        candidates.toSet == Set(Username("shared-player"), Username("unique-a"), Username("unique-b"))
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
      val config = makeConfig()

      for {
        _       <- seedDb
        _       <- RecruitmentConfig.upsert(config)
        runId   <- RecruitmentRun.insert(clubId, "default", T.t0)
        client  <- fakeChessComClient(responses)
        invited <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username("alice"), Username("bob")), config)
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
    test("respects inviteCap limit") {
      val responses = Map(
        "player/alice"   -> apiPlayerJson(200, "alice"),
        "player/bob"     -> apiPlayerJson(201, "bob"),
        "player/charlie" -> apiPlayerJson(202, "charlie")
      )
      val config = makeConfig()

      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        runId  <- RecruitmentRun.insert(clubId, "default", T.t0)
        client <- fakeChessComClient(responses)
        invited <- RecruitmentApp.evaluateCandidates(
          client,
          runId,
          clubUrlName,
          List(Username("alice"), Username("bob"), Username("charlie")),
          config,
          inviteCap = 2
        )
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        invited.size == 2,
        candidates.size == 2
      )
    },
    test("records Error outcome on API failure") {
      val responses = Map(
        "player/alice" -> apiPlayerJson(200, "alice")
      )
      val config = makeConfig()

      for {
        _          <- seedDb
        _          <- RecruitmentConfig.upsert(config)
        runId      <- RecruitmentRun.insert(clubId, "default", T.t0)
        client     <- fakeChessComClient(responses, failures = Set("bob"))
        invited    <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username("alice"), Username("bob")), config)
        candidates <- RecruitmentCandidate.selectByRun(runId)
        errors = candidates.filter(_.outcome == CandidateOutcome.Error)
      } yield assertTrue(
        invited.size == 1,
        invited.head == Username("alice"),
        candidates.size == 2,
        errors.size == 1,
        errors.head.username == Username("bob"),
        errors.head.rejectionReason.isDefined
      )
    }
  )

  // ==========================================================================
  // Suite: Filter chain
  // ==========================================================================

  /** Helper: run evaluateCandidates with a single candidate and return the outcome. */
  private def evalSingle(
      responses: Map[String, String],
      config: RecruitmentConfig,
      username: String = "alice"
    ): ZIO[Transactor, Throwable, CandidateOutcome] =
    for {
      _      <- seedDb
      _      <- RecruitmentConfig.upsert(config)
      runId  <- RecruitmentRun.insert(clubId, "default", Instant.now())
      client <- fakeChessComClient(responses)
      _      <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username.wrap(username)), config)
      cands  <- RecruitmentCandidate.selectByRun(runId)
    } yield cands.head.outcome

  private def suiteFilterChain = suite("filter chain")(
    test("rejects closed account") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", status = "closed"))
      for { outcome <- evalSingle(responses, makeConfig()) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by minDaysSinceRegistration") {
      // Player joined 5 days ago, config requires 30 days
      val recentJoin = Instant.now().minus(java.time.Duration.ofDays(5)).getEpochSecond
      val responses  = Map("player/alice" -> apiPlayerJson(200, "alice", joined = recentJoin))
      val config     = makeConfig().copy(minDaysSinceRegistration = Some(30))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player meeting minDaysSinceRegistration") {
      // Player joined 60 days ago, config requires 30 days
      val oldJoin   = Instant.now().minus(java.time.Duration.ofDays(60)).getEpochSecond
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", joined = oldJoin))
      val config    = makeConfig().copy(minDaysSinceRegistration = Some(30))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by nationality exclude mode") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "XX"))
      val config    = makeConfig().copy(nationalityMode = Some("exclude"), nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player not in nationality exclude list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "ZZ"))
      val config    = makeConfig().copy(nationalityMode = Some("exclude"), nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by nationality include mode when not in list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "ZZ"))
      val config    = makeConfig().copy(nationalityMode = Some("include"), nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player in nationality include list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "YY"))
      val config    = makeConfig().copy(nationalityMode = Some("include"), nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by dailyMinElo") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 800)
      )
      val config = makeConfig().copy(dailyMinElo = Some(1000))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by dailyMaxElo") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 2200)
      )
      val config = makeConfig().copy(dailyMaxElo = Some(2000))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player within Elo range") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 1500)
      )
      val config = makeConfig().copy(dailyMinElo = Some(1000), dailyMaxElo = Some(2000))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by dailyMaxTimeoutPercent") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timeoutPct = 15.0)
      )
      val config = makeConfig().copy(dailyMaxTimeoutPercent = Some(10.0))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by dailyMinGamesFinished") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 5, losses = 3, draws = 2) // 10 games
      )
      val config = makeConfig().copy(dailyMinGamesFinished = Some(50))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by dailyMaxHoursPerMove") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timePerMove = 86400) // 24 hours
      )
      val config = makeConfig().copy(dailyMaxHoursPerMove = Some(12))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player within dailyMaxHoursPerMove") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timePerMove = 36000) // 10 hours
      )
      val config = makeConfig().copy(dailyMaxHoursPerMove = Some(12))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by maxClubs") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/clubs" -> apiPlayerClubsJson(List("club-1", "club-2", "club-3", "club-4", "club-5", "club-6"))
      )
      val config = makeConfig().copy(maxClubs = Some(5))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by excludeClubs") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/clubs" -> apiPlayerClubsJson(List("good-club", "banned-club"))
      )
      val config = makeConfig(excludeClubs = List("banned-club"))
      for { outcome <- evalSingle(responses, config) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects when player has match against target club") {
      val matchId = "https://api.chess.com/pub/match/12345"
      val responses = Map(
        "player/alice"           -> apiPlayerJson(200, "alice"),
        "player/alice/matches"   -> apiPlayerMatchesJson(registeredIds = List(matchId)),
        s"club/$clubUrlName/matches" -> apiClubMatchesJson(registeredIds = List(matchId))
      )
      for { outcome <- evalSingle(responses, makeConfig()) }
      yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts when player matches don't overlap target club") {
      val responses = Map(
        "player/alice"               -> apiPlayerJson(200, "alice"),
        "player/alice/matches"       -> apiPlayerMatchesJson(registeredIds = List("https://api.chess.com/pub/match/999")),
        s"club/$clubUrlName/matches" -> apiClubMatchesJson(registeredIds = List("https://api.chess.com/pub/match/888"))
      )
      for { outcome <- evalSingle(responses, makeConfig()) }
      yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by daysSinceLastInvited") {
      val config = makeConfig().copy(daysSinceLastInvited = Some(30))
      for {
        _     <- seedDb
        _     <- RecruitmentConfig.upsert(config)
        // Create a prior run with alice invited recently
        priorRunId <- RecruitmentRun.insert(clubId, "default", Instant.now())
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(priorRunId, Username("alice"), Instant.now(), CandidateOutcome.Invited, None)
        )
        // Now evaluate alice again
        runId  <- RecruitmentRun.insert(clubId, "default", Instant.now())
        client <- fakeChessComClient(Map("player/alice" -> apiPlayerJson(200, "alice")))
        _      <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username("alice")), config)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Rejected)
    },
    test("excludeSourceAdmins removes admin from candidate pool") {
      val responses = Map(
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        "club/source-club" -> apiClubJson(sourceClubId, "source-club", admins = List("admin-user")),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("admin-user", T.t0.getEpochSecond),
            ("regular-user", T.t0.getEpochSecond)
          )
        )
      )
      val config = makeConfig().copy(excludeSourceAdmins = true)

      for {
        _          <- seedDb
        client     <- fakeChessComClient(responses)
        candidates <- RecruitmentApp.gatherCandidates(client, clubUrlName, config, List(ClubUrlName("source-club")))
      } yield assertTrue(
        candidates.size == 1,
        candidates.head == Username("regular-user")
      )
    },
    test("excludeSourceAdmins=false keeps admins in candidate pool") {
      val responses = Map(
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        "club/source-club" -> apiClubJson(sourceClubId, "source-club", admins = List("admin-user")),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("admin-user", T.t0.getEpochSecond),
            ("regular-user", T.t0.getEpochSecond)
          )
        )
      )
      val config = makeConfig().copy(excludeSourceAdmins = false)

      for {
        _          <- seedDb
        client     <- fakeChessComClient(responses)
        candidates <- RecruitmentApp.gatherCandidates(client, clubUrlName, config, List(ClubUrlName("source-club")))
      } yield assertTrue(
        candidates.size == 2,
        candidates.toSet == Set(Username("admin-user"), Username("regular-user"))
      )
    },
    test("cache is populated after evaluation") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val config    = makeConfig()
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        runId  <- RecruitmentRun.insert(clubId, "default", Instant.now())
        client <- fakeChessComClient(responses)
        _      <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username("alice")), config)
        cached <- PlayerRecruitmentCache.selectId(pid0)
      } yield assertTrue(
        cached.isDefined,
        cached.get.clubCount.contains(0),
        cached.get.ongoingGames == 0,
        cached.get.dailyElo.contains(1200),
        cached.get.lastDailyTimeoutAt.isEmpty,
        cached.get.lastTmTimeoutAt.isEmpty
      )
    },
    test("former member rejected when excludeFormerMembers = true") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val config    = makeConfig(excludeFormerMembers = true)
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        // Seed alice as a former member of the club (player row needed for FK)
        _      <- SqlZioTypes.connectZIO {
          sql"""INSERT INTO player (player_id, joined) VALUES ($pid0, ${T.t0})
                ON CONFLICT (player_id) DO NOTHING""".update.run()
        }
        _      <- ClubMember.insert(ClubMember(clubId, pid0, T.t0, Some(T.t1)))
        runId  <- RecruitmentRun.insert(clubId, "default", Instant.now())
        client <- fakeChessComClient(responses)
        _      <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username("alice")), config)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Rejected)
    },
    test("former member accepted when excludeFormerMembers = false") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val config    = makeConfig(excludeFormerMembers = false)
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        // Seed alice as a former member of the club
        _      <- SqlZioTypes.connectZIO {
          sql"""INSERT INTO player (player_id, joined) VALUES ($pid0, ${T.t0})
                ON CONFLICT (player_id) DO NOTHING""".update.run()
        }
        _      <- ClubMember.insert(ClubMember(clubId, pid0, T.t0, Some(T.t1)))
        runId  <- RecruitmentRun.insert(clubId, "default", Instant.now())
        client <- fakeChessComClient(responses)
        _      <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username("alice")), config)
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
      val config    = makeConfig()
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        // Blacklist alice (indefinite)
        _      <- RecruitmentBlacklist.insert(
          RecruitmentBlacklist(clubId, pid0, T.t0, expiresAt = None, reason = Some("banned"))
        )
        runId  <- RecruitmentRun.insert(clubId, "default", Instant.now())
        client <- fakeChessComClient(responses)
        _      <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username("alice")), config)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        cands.size == 1,
        cands.head.outcome == CandidateOutcome.Rejected
      )
    },
    test("expired blacklist entry does not reject the player") {
      val now       = Instant.now()
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val config    = makeConfig()
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        // Blacklist alice with an already-expired entry
        _      <- RecruitmentBlacklist.insert(
          RecruitmentBlacklist(clubId, pid0, T.t0, expiresAt = Some(now.minus(java.time.Duration.ofDays(1))), reason = Some("temp ban"))
        )
        runId  <- RecruitmentRun.insert(clubId, "default", now)
        client <- fakeChessComClient(responses)
        _      <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username("alice")), config)
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
      val futureInstant = T.t3
      val responses = Map(
        s"club/$blacklistClubUrlName" -> apiClubJson(700, blacklistClubUrlName, Nil),
        "player/target-player"       -> apiPlayerJson(203, "target-player")
      )
      for {
        _       <- seedDb
        client  <- fakeChessComClient(responses)
        xa      <- ZIO.service[Transactor]
        _       <- BlacklistApp.addToBlacklist(blacklistClubUrlName, Username("target-player"), Some("toxic"), Some(futureInstant))
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
        s"club/$blacklistClubUrlName" -> apiClubJson(700, blacklistClubUrlName, Nil),
        "player/target-player"       -> apiPlayerJson(203, "target-player")
      )
      for {
        _       <- seedDb
        client  <- fakeChessComClient(responses)
        xa      <- ZIO.service[Transactor]
        _       <- BlacklistApp.addToBlacklist(blacklistClubUrlName, Username("target-player"), None, None)
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
        s"club/$freshClubUrlName" -> apiClubJson(701, freshClubUrlName, Nil),
        "player/target-player"   -> apiPlayerJson(203, "target-player")
      )
      for {
        _       <- seedDb
        client  <- fakeChessComClient(responses)
        xa      <- ZIO.service[Transactor]
        before  <- Club.selectId(freshClubId)
        _       <- BlacklistApp.addToBlacklist(freshClubUrlName, Username("target-player"), None, None)
                     .provideEnvironment(zio.ZEnvironment(client, xa))
        after   <- Club.selectId(freshClubId)
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
      config: RecruitmentConfig,
      cache: PlayerRecruitmentCache,
      username: String = "alice"
    ): ZIO[Transactor, Throwable, CandidateOutcome] =
    for {
      _      <- seedDb
      // Seed player row for FK constraint, then seed cache
      _      <- SqlZioTypes.connectZIO {
        sql"""INSERT INTO player (player_id, joined)
              VALUES (${cache.playerId}, ${T.t0})
              ON CONFLICT (player_id) DO NOTHING""".update.run()
      }
      _      <- PlayerRecruitmentCache.upsert(cache)
      _      <- RecruitmentConfig.upsert(config)
      runId  <- RecruitmentRun.insert(clubId, "default", Instant.now())
      client <- fakeChessComClient(responses)
      _      <- RecruitmentApp.evaluateCandidates(client, runId, clubUrlName, List(Username.wrap(username)), config)
      cands  <- RecruitmentCandidate.selectByRun(runId)
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
        ongoingGames = 3,
        ongoingTeamMatches = 2,
        tmGamesFinished90d = 10,
        tmTimeoutPct90d = Some(0.0),
        lastDailyTimeoutAt = Some(now.minus(java.time.Duration.ofDays(100))), // had a timeout once
        lastTmTimeoutAt = None
      )
      val config = makeConfig().copy(dailyMaxTimeoutPercent = Some(0.0))
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))

      for {
        outcome <- evalSingleWithCache(responses, config, staleCache)
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
        ongoingGames = 3,
        ongoingTeamMatches = 2,
        tmGamesFinished90d = 10,
        tmTimeoutPct90d = Some(0.0),
        lastDailyTimeoutAt = None,
        lastTmTimeoutAt = None
      )
      val config = makeConfig().copy(maxClubs = Some(50))
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))

      for {
        outcome <- evalSingleWithCache(responses, config, cache48h)
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
        ongoingGames = 0,
        ongoingTeamMatches = 0,
        tmGamesFinished90d = 0,
        tmTimeoutPct90d = None,
        lastDailyTimeoutAt = None,
        lastTmTimeoutAt = None
      )
      val config = makeConfig().copy(
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
        outcome <- evalSingleWithCache(responses, config, staleCache)
      } yield assertTrue(outcome == CandidateOutcome.Invited)
    }
  )

  // ==========================================================================
  // Suite: Full workflow (end-to-end)
  // ==========================================================================

  private def suiteFullWorkflow = suite("full workflow")(
    test("recruit end-to-end") {
      val responses = Map(
        s"club/$clubUrlName" -> apiClubJson(clubId, clubUrlName),
        s"club/$clubUrlName/members" -> apiClubMembersJson(
          List(
            ("existing", T.t0.getEpochSecond)
          )
        ),
        "club/source-club" -> apiClubJson(sourceClubId, "source-club"),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("existing", T.t0.getEpochSecond),
            ("candidate-a", T.t0.getEpochSecond),
            ("candidate-b", T.t0.getEpochSecond)
          )
        ),
        "player/existing" -> apiPlayerJson(199, "existing"),
        "player/candidate-a" -> apiPlayerJson(200, "candidate-a"),
        "player/candidate-b" -> apiPlayerJson(201, "candidate-b")
      )
      val config = makeConfig()

      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
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
  // Suite: Report mode
  // ==========================================================================

  private def suiteReport = suite("report mode")(
    test("showReport displays invited candidates") {
      for {
        _     <- seedDb
        runId <- RecruitmentRun.insert(clubId, "default", T.t0)
        _     <- RecruitmentRun.update(RecruitmentRun(runId, clubId, "default", T.t0, Some(T.t1), 2))
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, Username("alice"), T.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, Username("bob"), T.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentApp.showReport(clubUrlName, Some(runId.toString))
      } yield assertTrue(true)
    },
    test("showReport with latest run") {
      for {
        _     <- seedDb
        runId <- RecruitmentRun.insert(clubId, "default", T.t0)
        _     <- RecruitmentRun.update(RecruitmentRun(runId, clubId, "default", T.t0, Some(T.t1), 1))
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, Username("alice"), T.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentApp.showReport(clubUrlName, None)
      } yield assertTrue(true)
    }
  )
}
