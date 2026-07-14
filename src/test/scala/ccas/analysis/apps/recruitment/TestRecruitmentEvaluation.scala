package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant}

import ccas.utils.sql.PostgresClient
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.{RIO, ZLayer}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{Elo, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.sql.FreshSchemaLayer

object TestRecruitmentEvaluation extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentEvaluation")(
    suiteEvaluateCandidates,
    suiteCacheFilters
  ).provideShared(
    FreshSchemaLayer("test_recruitment_evaluation", onInit = Tables.ensureTables),
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

  private def testPersistsPlayerSnapshotAndCandidate = test("persists Player, PlayerSnapshot, and RecruitmentCandidate") {
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
      client     <- fakeChessComClient(responses, failures = Set("bob"))
      invited    <- evalCandidates(client, runId, List(Username("alice"), Username("bob")), criteria)
      candidates <- RecruitmentCandidate.selectByRun(runId)
    } yield assertTrue(
      invited.size == 1,
      invited.head == Username("alice"),
      candidates.size == 1,
      candidates.head.outcome == CandidateOutcome.Deferred
    )
  }

  private def testMidPipelineErrorPersistsCandidateWithErrorOutcome = test("mid-pipeline error persists candidate with Error outcome") {
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
      candidates.head.rejectionReason.isDefined
    )
  }


  // ==========================================================================
  // Suite: Cache-aware filters
  // ==========================================================================

  /** Helper: run evalSingle but seed a cache row (and its player FK) before evaluation. */
  private def evalSingleWithCache(
    responses: Map[String, String],
    criteria: RecruitmentCriteria,
    cache: PlayerRecruitmentCache,
    username: String = "alice"
  ): RIO[ProgressDisplay & PostgresClient, Option[CandidateOutcome]] =
    for {
      _ <- seedDb
      // Seed player row for FK constraint, then seed cache
      _          <- seedPlayer(cache.playerId)
      _          <- PlayerRecruitmentCache.upsert(cache)
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
      client     <- fakeChessComClient(responses)
      _          <- evalCandidates(client, runId, List(Username.wrap(username)), criteria)
      cands      <- RecruitmentCandidate.selectByRun(runId)
    } yield cands.headOption.map(_.outcome)

  private def suiteCacheFilters = suite("cache-aware filters")(
    testZeroToleranceDailyTimeoutRejectsFromCache,
    testMaxClubsCacheRejectionAt48hOldCache,
    testStaleCacheFallsThroughToApiChecks,
    testCacheOnlyRejectionDoesNotBlockReEvaluation
  )

  private def testZeroToleranceDailyTimeoutRejectsFromCache = test("zero-tolerance daily timeout rejects from cache") {
    val now = Instant.now()
    val staleCache = PlayerRecruitmentCache(
      playerId = pid0,
      fetchedAt = now.minus(java.time.Duration.ofDays(30)), // very old cache
      dailyElo = Some(Elo(1500)),
      dailyScoreRate = None,
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
    } yield assertTrue(outcome.isEmpty) // cache-only rejection: no candidate row persisted
  }

  private def testMaxClubsCacheRejectionAt48hOldCache = test("maxClubs cache rejection at 48h old cache") {
    val now = Instant.now()
    val cache48h = PlayerRecruitmentCache(
      playerId = pid0,
      fetchedAt = now.minus(java.time.Duration.ofHours(48)),
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
      outcome <- evalSingleWithCache(responses, criteria, cache48h)
    } yield assertTrue(outcome.isEmpty) // cache-only rejection: no candidate row persisted
  }

  private def testStaleCacheFallsThroughToApiChecks = test("stale cache falls through to API checks") {
    val now = Instant.now()
    val staleCache = PlayerRecruitmentCache(
      playerId = pid0,
      fetchedAt = now.minus(java.time.Duration.ofDays(31)),
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
      outcome <- evalSingleWithCache(responses, criteria, staleCache)
    } yield assertTrue(outcome.contains(CandidateOutcome.Deferred))
  }

  private def testCacheOnlyRejectionDoesNotBlockReEvaluation = test("cache-only rejection does not block re-evaluation via daysSinceRejected") {
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
      // First run: cache-only rejection (no candidate row written)
      outcome1 <- evalSingleWithCache(responses, criteria, cache)
      // Verify no Rejected row exists for daysSinceRejected to find
      rejected <- RecruitmentCandidate.selectLatestRejectedByAlias(pid0, clubId, "default")
    } yield assertTrue(
      outcome1.isEmpty,
      rejected.isEmpty
    )
  }
}
