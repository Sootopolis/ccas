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

  // --- Helpers ---

  private def apiPlayerJson(
      playerId: Long,
      username: String,
      status: String = "basic",
      joined: Long = T.t0.getEpochSecond
    ): String = {
    val fields = List(
      s""""player_id": $playerId""",
      s""""username": "$username"""",
      s""""country": "https://api.chess.com/pub/country/US"""",
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

  private def apiClubJson(clubId: Long, urlName: String): String =
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
       |  "admin": [],
       |  "description": "A test club"
       |}""".stripMargin

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
        // Player endpoint
        Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
          if failures.contains(username) then Response(status = Status.NotFound)
          else responses.get(s"player/$username").fold(Response(status = Status.NotFound))(Response.json(_))
        },
        // Club endpoint
        Method.GET / "pub" / "club" / string("club") -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName").fold(Response(status = Status.NotFound))(Response.json(_))
        },
        // Club members endpoint
        Method.GET / "pub" / "club" / string("club") / "members" -> handler { (clubName: String, _: Request) =>
          responses.get(s"club/$clubName/members").fold(Response(status = Status.NotFound))(Response.json(_))
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
      _ <- SqlZioTypes.connectZIO(sql"ALTER TABLE player ADD COLUMN IF NOT EXISTS board_url VARCHAR".update.run())
      // Clean up test data
      _ <- RecruitmentCandidate.deleteAll
      _ <- RecruitmentRun.deleteAll
      _ <- RecruitmentConfig.deleteAll
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_member WHERE club_id = $clubId".update.run())
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_member WHERE club_id = $sourceClubId".update.run())
      _ <- ZIO.foreachDiscard(List(pid0, pid1, pid2)) { pid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run()) *>
          SqlZioTypes.connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- Club.upsert(club)
    } yield ()

  private def makeConfig(
      maxCandidates: Int = 10,
      sourceClubs: List[String] = List("source-club"),
      excludeClubs: List[String] = Nil,
      onExhaustion: ExhaustionBehavior = ExhaustionBehavior.Stop
    ): RecruitmentConfig =
    RecruitmentConfig(
      clubId = clubId,
      configName = "default",
      maxCandidates = maxCandidates,
      sourceClubs = sourceClubs,
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
      daysSinceLastInvited = None
    )

  // --- Spec ---

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentApp")(
    suiteConfigHelpers,
    suiteDbCrud,
    suiteGatherCandidates,
    suiteEvaluateCandidates,
    suiteFullWorkflow,
    suiteReport
  ).provideShared(
    DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  // ==========================================================================
  // Suite: Config helper methods (pure)
  // ==========================================================================

  private def suiteConfigHelpers = suite("config helpers")(
    test("sourceClubNames wraps strings to ClubUrlName") {
      val config = makeConfig(sourceClubs = List("club-a", "club-b"))
      assertTrue(
        config.sourceClubNames == List(ClubUrlName("club-a"), ClubUrlName("club-b"))
      )
    },
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
    }
  )

  // ==========================================================================
  // Suite: DB CRUD
  // ==========================================================================

  private def suiteDbCrud = suite("DB CRUD")(
    test("RecruitmentConfig upsert and select") {
      val config = makeConfig(sourceClubs = List("club-a", "club-b"), excludeClubs = List("club-x"))
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        loaded <- RecruitmentConfig.select(clubId, "default")
      } yield assertTrue(
        loaded.isDefined,
        loaded.get.maxCandidates == 10,
        loaded.get.sourceClubs == List("club-a", "club-b"),
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
      val config  = makeConfig(maxCandidates = 5)
      val updated = config.copy(maxCandidates = 20)
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        _      <- RecruitmentConfig.upsert(updated)
        loaded <- RecruitmentConfig.select(clubId, "default")
      } yield assertTrue(loaded.get.maxCandidates == 20)
    },
    test("RecruitmentConfig TEXT[] array round-trip") {
      val config = makeConfig(
        sourceClubs = List("alpha", "beta", "gamma"),
        excludeClubs = List("delta")
      ).copy(nationalityCountries = List("US", "GB", "DE"))
      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        loaded <- RecruitmentConfig.select(clubId, "default")
      } yield assertTrue(
        loaded.get.sourceClubs == List("alpha", "beta", "gamma"),
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
        candidates <- RecruitmentApp.gatherCandidates(client, clubId, clubUrlName, config)
      } yield assertTrue(
        candidates.size == 2,
        !candidates.contains(Username("existing-member")),
        candidates.toSet == Set(Username("candidate-a"), Username("candidate-b"))
      )
    },
    test("deduplicates across multiple source clubs") {
      val responses = Map(
        s"club/$clubUrlName/members" -> apiClubMembersJson(Nil),
        "club/source-a/members" -> apiClubMembersJson(
          List(
            ("shared-player", T.t0.getEpochSecond),
            ("unique-a", T.t0.getEpochSecond)
          )
        ),
        "club/source-b/members" -> apiClubMembersJson(
          List(
            ("shared-player", T.t0.getEpochSecond),
            ("unique-b", T.t0.getEpochSecond)
          )
        )
      )
      val config = makeConfig(sourceClubs = List("source-a", "source-b"))

      for {
        _          <- seedDb
        client     <- fakeChessComClient(responses)
        candidates <- RecruitmentApp.gatherCandidates(client, clubId, clubUrlName, config)
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

      for {
        _       <- seedDb
        runId   <- RecruitmentRun.insert(clubId, "default", T.t0)
        client  <- fakeChessComClient(responses)
        invited <- RecruitmentApp.evaluateCandidates(client, runId, List(Username("alice"), Username("bob")), 10)
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
    test("respects maxCandidates limit") {
      val responses = Map(
        "player/alice"   -> apiPlayerJson(200, "alice"),
        "player/bob"     -> apiPlayerJson(201, "bob"),
        "player/charlie" -> apiPlayerJson(202, "charlie")
      )

      for {
        _      <- seedDb
        runId  <- RecruitmentRun.insert(clubId, "default", T.t0)
        client <- fakeChessComClient(responses)
        invited <- RecruitmentApp.evaluateCandidates(
          client,
          runId,
          List(Username("alice"), Username("bob"), Username("charlie")),
          2
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

      for {
        _          <- seedDb
        runId      <- RecruitmentRun.insert(clubId, "default", T.t0)
        client     <- fakeChessComClient(responses, failures = Set("bob"))
        invited    <- RecruitmentApp.evaluateCandidates(client, runId, List(Username("alice"), Username("bob")), 10)
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
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("existing", T.t0.getEpochSecond),
            ("candidate-a", T.t0.getEpochSecond),
            ("candidate-b", T.t0.getEpochSecond)
          )
        ),
        "player/candidate-a" -> apiPlayerJson(200, "candidate-a"),
        "player/candidate-b" -> apiPlayerJson(201, "candidate-b")
      )
      val config = makeConfig()

      for {
        _      <- seedDb
        _      <- RecruitmentConfig.upsert(config)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        result <- RecruitmentApp.recruit(clubUrlName, "default").provideEnvironment(zio.ZEnvironment(client, xa))
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
