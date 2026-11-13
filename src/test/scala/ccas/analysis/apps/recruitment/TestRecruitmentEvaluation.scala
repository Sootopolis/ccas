package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant}
import java.time.temporal.ChronoUnit

import zio.{RIO, ZLayer}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{Elo, PlayerId, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.BodyStore
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient

object TestRecruitmentEvaluation extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentEvaluation")(
    suiteEvaluateCandidates,
    suiteCacheFilters,
    suiteTmTimeouts
  ).provideShared(
    FreshSchemaLayer(schema = "test_recruitment_evaluation", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: Evaluate candidates
  // ==========================================================================

  private def suiteEvaluateCandidates = suite("evaluateCandidates")(
    testPersistsPlayerSnapshotAndCandidate,
    testRespectsTargetLimit,
    testSkipsCandidateOnApiFailure,
    testMidPipelineErrorPersistsCandidateWithErrorOutcome
  )

  private def testPersistsPlayerSnapshotAndCandidate =
    test("persists Player, PlayerSnapshot, and RecruitmentCandidate") {
      val responses = Map(
        "player/alice" -> apiPlayerJson(200, "alice"),
        "player/bob"   -> apiPlayerJson(201, "bob")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
        client     <- fakeChessComClient(responses)
        invited    <- evalCandidates(client, runId, List(Username("alice"), Username("bob")), criteria)
        // Check invited list
        _ = assertTrue(invited.size == 2)
        // Check Player table (now includes current username/status/title)
        playerA <- Player.selectId(pid0)
        playerB <- Player.selectId(pid1)
        // Check RecruitmentCandidate table
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        playerA.isDefined,
        playerB.isDefined,
        playerA.get.username == Username("alice"),
        playerB.get.username == Username("bob"),
        candidates.size == 2,
        candidates.forall(_.outcome == CandidateOutcome.Deferred)
      )
    }

  private def testRespectsTargetLimit = test("respects target limit") {
    val responses = Map(
      "player/alice"   -> apiPlayerJson(200, "alice"),
      "player/bob"     -> apiPlayerJson(201, "bob"),
      "player/charlie" -> apiPlayerJson(202, "charlie")
    )
    val criteria = makeCriteria()

    for {
      _          <- seedDb
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
      client     <- fakeChessComClient(responses)
      invited <- evalCandidates(
        client = client,
        runId = runId,
        candidates = List(Username("alice"), Username("bob"), Username("charlie")),
        criteria = criteria,
        target = 2
      )
      candidates <- RecruitmentCandidate.selectByRun(runId)
    } yield assertTrue(
      invited.size == 2,
      candidates.size == 2
    )
  }

  private def testSkipsCandidateOnApiFailure = test("skips candidate on API failure (no record written)") {
    val responses = Map(
      "player/alice" -> apiPlayerJson(200, "alice")
    )
    val criteria = makeCriteria()

    for {
      _          <- seedDb
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
      client     <- fakeChessComClient(responses = responses, failures = Set("bob"))
      invited    <- evalCandidates(client, runId, List(Username("alice"), Username("bob")), criteria)
      candidates <- RecruitmentCandidate.selectByRun(runId)
    } yield assertTrue(
      invited.size == 1,
      invited.head == Username("alice"),
      candidates.size == 1,
      candidates.head.outcome == CandidateOutcome.Deferred
    )
  }

  private def testMidPipelineErrorPersistsCandidateWithErrorOutcome =
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
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username("alice")), criteria)
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Error,
        candidates.head.playerId == pid0,
        candidates.head.errorMessage.isDefined,
        candidates.head.rejectionReason.isEmpty
      )
    }

  // ==========================================================================
  // Suite: Cache-aware filters
  // ==========================================================================

  /** Seeds `cache` and its player, leaving the evaluations to [[evaluateOnce]]. */
  private def seedCache(cache: PlayerRecruitmentCache): RIO[PostgresClient, Unit] =
    seedDb *> seedPlayer(cache.playerId) *> PlayerRecruitmentCache.upsert(cache).unit

  /** Evaluates alice in a run of its own, returning the outcome and reason written for her, if any. */
  private def evaluateOnce(
    responses: Map[String, String],
    criteria: RecruitmentCriteria
  ): RIO[ProgressDisplay & PostgresClient & BodyStore, Option[Recorded]] =
    for {
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
      client     <- fakeChessComClient(responses)
      _          <- evalCandidates(client, runId, List(Username("alice")), criteria)
      candidates <- RecruitmentCandidate.selectByRun(runId)
    } yield candidates.headOption.map(recordedOf)

  private def evalSingleWithCache(
    responses: Map[String, String],
    criteria: RecruitmentCriteria,
    cache: PlayerRecruitmentCache
  ): RIO[ProgressDisplay & PostgresClient & BodyStore, Option[Recorded]] =
    seedCache(cache) *> evaluateOnce(responses, criteria)

  private def suiteCacheFilters = suite("cache-aware filters")(
    testDailyTimeoutInsideWindowRejectsFromCache,
    testDailyTimeoutOutsideWindowDoesNotRejectFromCache,
    testTmTimeoutInsideWindowRejectsFromCache,
    testTmTimeoutOutsideWindowDoesNotRejectFromCache,
    testMaxClubsCacheRejectionAt48hOldCache,
    testStaleCacheFallsThroughToApiChecks,
    testCacheOnlyRejectionDoesNotBlockReEvaluation,
    testCacheRejectionCountsAsEvaluated,
    testDailyStatsRejectionKeepsNoEarlierFigure,
    testEarlierFigureDoesNotRejectFromRedatedRow,
    testRefreshedClubCountRejectsFromCacheNextTime,
    testRedatedRowKeepsEarlierTimeouts,
    testFreshRowKeepsItsFiguresAndDate
  )

  /** A row too old for its figures to reject from cache, holding 100 ongoing games. */
  private def staleRow(lastDailyTimeoutAt: Option[Instant], lastTmTimeoutAt: Option[Instant]): PlayerRecruitmentCache =
    cacheWithTimeouts(dailyTimeoutAgeDaysOption = None, tmTimeoutAgeDaysOption = None).copy(
      ongoingGames = Some(100),
      lastDailyTimeoutAt = lastDailyTimeoutAt,
      lastTmTimeoutAt = lastTmTimeoutAt
    )

  // A daily Elo of 1200 against a minimum of 1600: `CheckDailyStats` rejects, so the filters after it never run.
  private val aliceAt1200 = Map(
    "player/alice"       -> apiPlayerJson(200, "alice"),
    "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 1200)
  )
  private val dailyStatsRejectsAlice = makeCriteria().copy(dailyMinElo = Some(Elo(1600)))

  private def testDailyStatsRejectionKeepsNoEarlierFigure =
    test("a daily-stats rejection keeps no figure from an earlier evaluation") {
      val seeded = staleRow(lastDailyTimeoutAt = None, lastTmTimeoutAt = None)
      for {
        _         <- seedCache(seeded)
        recorded  <- evaluateOnce(aliceAt1200, dailyStatsRejectsAlice)
        rowOption <- PlayerRecruitmentCache.selectId(pid0)
        row = rowOption.get
      } yield assertTrue(
        recorded.contains(rejectedFor(RejectionReason.DailyMinElo)),
        row.fetchedAt.isAfter(seeded.fetchedAt),
        row.dailyElo.contains(Elo(1200)),
        row.dailyScoreRate.isDefined,
        row.clubCount.contains(0), // the seeded row holds 5
        row.ongoingGames.isEmpty,
        row.ongoingTeamMatches.isEmpty,
        row.tmGamesFinished90d.isEmpty,
        row.tmTimeoutPct90d.isEmpty
      )
    }

  private def testEarlierFigureDoesNotRejectFromRedatedRow =
    test("a figure from an earlier evaluation does not reject from cache after the row is re-dated") {
      // The seeded 100 ongoing games would fail this limit; Chess.com now reports none.
      val ongoingLimit = makeCriteria().copy(dailyMaxOngoingGames = Some(60))
      for {
        _      <- seedCache(staleRow(lastDailyTimeoutAt = None, lastTmTimeoutAt = None))
        first  <- evaluateOnce(aliceAt1200, dailyStatsRejectsAlice)
        second <- evaluateOnce(aliceAt1200, ongoingLimit)
      } yield assertTrue(
        first.contains(rejectedFor(RejectionReason.DailyMinElo)),
        second.contains(passed)
      )
    }

  private def testRefreshedClubCountRejectsFromCacheNextTime =
    test("a club count refreshed on a stale row rejects from cache next time") {
      val responses = aliceAt1200 + ("player/alice/clubs" -> apiPlayerClubsJson(List("club-a", "club-b")))
      val criteria  = makeCriteria().copy(maxClubs = Some(1))
      val seeded    = staleRow(lastDailyTimeoutAt = None, lastTmTimeoutAt = None)
      for {
        _         <- seedCache(seeded)
        first     <- evaluateOnce(responses, criteria)
        rowOption <- PlayerRecruitmentCache.selectId(pid0)
        second    <- evaluateOnce(responses, criteria)
      } yield assertTrue(
        first.contains(rejectedFor(RejectionReason.MaxClubs)),
        rowOption.exists(_.fetchedAt.isAfter(seeded.fetchedAt)),
        rowOption.flatMap(_.clubCount).contains(2),
        second.contains(rejectedFromCacheFor(RejectionReason.MaxClubs))
      )
    }

  private def testRedatedRowKeepsEarlierTimeouts =
    test("a re-dated row keeps the timeouts of earlier evaluations") {
      // Whole seconds, so the values survive the database's microsecond precision unchanged.
      val now          = Instant.now().truncatedTo(ChronoUnit.SECONDS)
      val dailyTimeout = now.minus(Duration.ofDays(20))
      val tmTimeout    = now.minus(Duration.ofDays(10))
      for {
        _         <- seedCache(staleRow(lastDailyTimeoutAt = Some(dailyTimeout), lastTmTimeoutAt = Some(tmTimeout)))
        _         <- evaluateOnce(aliceAt1200, dailyStatsRejectsAlice)
        rowOption <- PlayerRecruitmentCache.selectId(pid0)
      } yield assertTrue(
        rowOption.flatMap(_.lastDailyTimeoutAt).contains(dailyTimeout),
        rowOption.flatMap(_.lastTmTimeoutAt).contains(tmTimeout)
      )
    }

  private def testFreshRowKeepsItsFiguresAndDate =
    test("a fresh row keeps its figures and its date when a filter refreshes one") {
      // Whole seconds, so the date survives the database's microsecond precision unchanged.
      val fiveDaysAgo = Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(Duration.ofDays(5))
      val seeded = cacheWithTimeouts(dailyTimeoutAgeDaysOption = None, tmTimeoutAgeDaysOption = None)
        .copy(fetchedAt = fiveDaysAgo, clubCount = Some(0))
      // Two clubs against a limit of one: `CheckClubs` refreshes the count, then rejects.
      val responses = aliceAt1200 + ("player/alice/clubs" -> apiPlayerClubsJson(List("club-a", "club-b")))
      for {
        _         <- seedCache(seeded)
        recorded  <- evaluateOnce(responses, makeCriteria().copy(maxClubs = Some(1)))
        rowOption <- PlayerRecruitmentCache.selectId(pid0)
      } yield assertTrue(
        recorded.contains(rejectedFor(RejectionReason.MaxClubs)),
        rowOption.map(_.fetchedAt).contains(fiveDaysAgo),
        rowOption.flatMap(_.clubCount).contains(2),
        rowOption.flatMap(_.dailyElo) == seeded.dailyElo,
        rowOption.flatMap(_.ongoingGames) == seeded.ongoingGames
      )
    }

  /** A 30-day-old row whose figures are all clean, so only the timeouts given can reject from cache. */
  private def cacheWithTimeouts(
    dailyTimeoutAgeDaysOption: Option[Long],
    tmTimeoutAgeDaysOption: Option[Long]
  ): PlayerRecruitmentCache = {
    val now = Instant.now()
    PlayerRecruitmentCache(
      playerId = pid0,
      fetchedAt = now.minus(Duration.ofDays(30)),
      dailyElo = Some(Elo(1500)),
      dailyScoreRate = None,
      dailyTimeoutPct = Some(0.0),
      dailyGamesFinished = Some(200),
      clubCount = Some(5),
      ongoingGames = Some(3),
      ongoingTeamMatches = Some(2),
      tmGamesFinished90d = Some(10),
      tmTimeoutPct90d = Some(0.0),
      lastDailyTimeoutAt = dailyTimeoutAgeDaysOption.map(days => now.minus(Duration.ofDays(days))),
      lastTmTimeoutAt = tmTimeoutAgeDaysOption.map(days => now.minus(Duration.ofDays(days)))
    )
  }

  /** Everything a full evaluation reads, all clean: a candidate that reaches it passes. */
  private def cleanEvaluationResponses: Map[String, String] =
    archivesInWindow("alice", Instant.now(), archiveJson(Nil)) ++ Map(
      "player/alice"       -> apiPlayerJson(200, "alice"),
      "player/alice/stats" -> apiPlayerStatsJson(timeoutPct = 0.0)
    )

  private val zeroDailyTolerance = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0))
  private val zeroTmTolerance    = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0))

  private def testDailyTimeoutInsideWindowRejectsFromCache =
    test("a zero-tolerance daily timeout inside the 90-day window rejects from cache") {
      val cache = cacheWithTimeouts(dailyTimeoutAgeDaysOption = Some(89), tmTimeoutAgeDaysOption = None)
      for {
        recorded <- evalSingleWithCache(cleanEvaluationResponses, zeroDailyTolerance, cache)
      } yield assertTrue(recorded.contains(rejectedFromCacheFor(RejectionReason.RecentDailyTimeout)))
    }

  private def testDailyTimeoutOutsideWindowDoesNotRejectFromCache =
    test("a zero-tolerance daily timeout older than 90 days does not reject from cache") {
      val cache = cacheWithTimeouts(dailyTimeoutAgeDaysOption = Some(91), tmTimeoutAgeDaysOption = None)
      for {
        recorded <- evalSingleWithCache(cleanEvaluationResponses, zeroDailyTolerance, cache)
      } yield assertTrue(recorded.contains(passed))
    }

  private def testTmTimeoutInsideWindowRejectsFromCache =
    test("a zero-tolerance team-match timeout inside the 90-day window rejects from cache") {
      val cache = cacheWithTimeouts(dailyTimeoutAgeDaysOption = None, tmTimeoutAgeDaysOption = Some(89))
      for {
        recorded <- evalSingleWithCache(cleanEvaluationResponses, zeroTmTolerance, cache)
      } yield assertTrue(recorded.contains(rejectedFromCacheFor(RejectionReason.RecentTmTimeout)))
    }

  private def testTmTimeoutOutsideWindowDoesNotRejectFromCache =
    test("a zero-tolerance team-match timeout older than 90 days does not reject from cache") {
      val cache = cacheWithTimeouts(dailyTimeoutAgeDaysOption = None, tmTimeoutAgeDaysOption = Some(91))
      for {
        recorded <- evalSingleWithCache(cleanEvaluationResponses, zeroTmTolerance, cache)
      } yield assertTrue(recorded.contains(passed))
    }

  private def testMaxClubsCacheRejectionAt48hOldCache = test("maxClubs cache rejection at 48h old cache") {
    val now = Instant.now()
    val cache48h = PlayerRecruitmentCache(
      playerId = pid0,
      fetchedAt = now.minus(Duration.ofHours(48)),
      dailyElo = Some(Elo(1500)),
      dailyScoreRate = None,
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
      recorded <- evalSingleWithCache(responses, criteria, cache48h)
    } yield assertTrue(recorded.contains(rejectedFromCacheFor(RejectionReason.MaxClubs)))
  }

  private def testStaleCacheFallsThroughToApiChecks = test("stale cache falls through to API checks") {
    val now = Instant.now()
    val staleCache = PlayerRecruitmentCache(
      playerId = pid0,
      fetchedAt = now.minus(Duration.ofDays(31)),
      dailyElo = Some(Elo(500)),
      dailyScoreRate = None,
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
      dailyMinElo = Some(Elo(1000)),
      dailyMaxTimeoutPercent = Some(10.0)
    )
    val responses = Map(
      "player/alice"       -> apiPlayerJson(200, "alice"),
      "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 1500, timeoutPct = 2.0),
      "player/alice/clubs" -> apiPlayerClubsJson(List("club-a", "club-b"))
    )

    for {
      recorded <- evalSingleWithCache(responses, criteria, staleCache)
    } yield assertTrue(recorded.contains(passed))
  }

  private def testCacheOnlyRejectionDoesNotBlockReEvaluation =
    test("cache-only rejection does not block re-evaluation via daysSinceRejected") {
      val now = Instant.now()
      val cache = PlayerRecruitmentCache(
        playerId = pid0,
        fetchedAt = now.minus(Duration.ofHours(12)),
        dailyElo = Some(Elo(1500)),
        dailyScoreRate = None,
        dailyTimeoutPct = Some(0.0),
        dailyGamesFinished = Some(200),
        clubCount = Some(120), // over limit
        ongoingGames = Some(3),
        ongoingTeamMatches = Some(2),
        tmGamesFinished90d = Some(10),
        tmTimeoutPct90d = Some(0.0),
        lastDailyTimeoutAt = None,
        lastTmTimeoutAt = None
      )
      val criteria  = makeCriteria(daysSinceRejected = Some(30)).copy(maxClubs = Some(50))
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))

      for {
        recorded <- evalSingleWithCache(responses, criteria, cache)
        rejected <- RecruitmentCandidate.selectLatestRejectedByAlias(pid0, clubId, "default")
      } yield assertTrue(
        recorded.contains(rejectedFromCacheFor(RejectionReason.MaxClubs)),
        rejected.isEmpty // the cooldown reads only `Rejected` rows
      )
    }

  private def testCacheRejectionCountsAsEvaluated =
    test("a cache rejection counts towards the run's evaluated total") {
      val cache = cacheWithTimeouts(dailyTimeoutAgeDaysOption = None, tmTimeoutAgeDaysOption = None)
        .copy(fetchedAt = Instant.now().minus(Duration.ofHours(12)), clubCount = Some(120))
      val criteria = makeCriteria().copy(maxClubs = Some(50))
      for {
        _          <- seedCache(cache)
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
        client     <- fakeChessComClient(Map("player/alice" -> apiPlayerJson(200, "alice")))
        _          <- evalCandidates(client, runId, List(Username("alice")), criteria)
        evaluated  <- RecruitmentCandidate.selectCountByRun(runId)
      } yield assertTrue(evaluated == 1)
    }

  // ==========================================================================
  // Suite: Team-match timeouts
  // ==========================================================================

  private def tmGame(white: String, whiteResult: String, blackResult: String): String =
    archiveGameJson(
      white = white,
      black = "opponent",
      whiteResult = whiteResult,
      blackResult = blackResult,
      endTime = Instant.now().minus(Duration.ofHours(9)).getEpochSecond,
      matchUrl = Some("https://www.chess.com/match/9999")
    )

  /** Evaluates `username` at zero tolerance for team-match timeouts, with Chess.com reporting an overall rate of 0. */
  private def evalTmGames(
    playerId: PlayerId,
    username: String,
    games: List[String]
  ): RIO[ProgressDisplay & PostgresClient & BodyStore, (Option[Recorded], Option[PlayerRecruitmentCache])] = {
    // Every month of the window serves the games, so a month rolling over mid-test cannot lose them.
    val responses = archivesInWindow(username, Instant.now(), archiveJson(games)) ++ Map(
      s"player/$username"       -> apiPlayerJson(PlayerId.unwrap(playerId), username),
      s"player/$username/stats" -> apiPlayerStatsJson(timeoutPct = 0.0)
    )
    val criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0))
    for {
      _          <- seedDb
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
      client     <- fakeChessComClient(responses)
      _          <- evalCandidates(client, runId, List(Username.wrap(username)), criteria)
      candidates <- RecruitmentCandidate.selectByRun(runId)
      cache      <- PlayerRecruitmentCache.selectId(playerId)
    } yield (candidates.headOption.map(recordedOf), cache)
  }

  private def suiteTmTimeouts = suite("team-match timeouts")(
    testTmTimeoutRejectsWhenOverallRateReadsZero,
    testCleanTmGamesPassAtZeroTolerance
  )

  private def testTmTimeoutRejectsWhenOverallRateReadsZero =
    test("a recent team-match timeout rejects the candidate and is counted, though the overall timeout rate reads 0") {
      val games = List(
        tmGame("alice", "timeout", "win"),
        tmGame("alice", "win", "checkmated"),
        tmGame("alice", "win", "checkmated"),
        tmGame("alice", "win", "checkmated")
      )
      for {
        (recorded, cache) <- evalTmGames(pid0, "alice", games)
      } yield assertTrue(
        recorded.contains(rejectedFor(RejectionReason.DailyMaxTmTimeoutPercent)),
        cache.flatMap(_.tmTimeoutPct90d).contains(25.0)
      )
    }

  private def testCleanTmGamesPassAtZeroTolerance =
    test("team-match games with no timeout pass at zero tolerance") {
      val games = List(
        tmGame("bob", "win", "checkmated"),
        tmGame("bob", "checkmated", "win")
      )
      for {
        (recorded, cache) <- evalTmGames(pid1, "bob", games)
      } yield assertTrue(
        recorded.contains(passed),
        cache.flatMap(_.tmTimeoutPct90d).contains(0.0)
      )
    }
}
