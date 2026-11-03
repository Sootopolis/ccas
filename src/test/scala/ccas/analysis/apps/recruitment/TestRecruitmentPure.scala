package ccas.analysis.apps.recruitment

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.TestTimes
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.makeCriteria
import ccas.analysis.tables.{PlayerRecruitmentCache, RecruitmentCriteria}
import ccas.api.misc.subtypes.{ClubSlug, Elo, PlayerId}

object TestRecruitmentPure extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentPure")(
    suiteParseRecruitArgs,
    suiteCriteriaHelpers,
    suite("cached criteria")(cachedCriteriaTests*)
  )

  // ==========================================================================
  // Suite: CLI argument parsing (pure)
  // ==========================================================================

  private def suiteParseRecruitArgs = suite("parseRecruitArgs")(
    test("alias and target from positional + flag") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target", "3"))
      assertTrue(
        r.alias == "default",
        r.target.contains(3),
        r.sourceClubs.isEmpty,
        !r.cumulative,
        !r.focus
      )
    },
    test("source clubs before flags") {
      val r = RecruitmentApp.parseRecruitArgs(List("myalias", "club-a", "club-b", "--target", "10"))
      assertTrue(
        r.alias == "myalias",
        r.sourceClubs.map(ClubSlug.unwrap) == List("club-a", "club-b"),
        r.target.contains(10)
      )
    },
    test("no flags defaults") {
      val r = RecruitmentApp.parseRecruitArgs(List("myalias"))
      assertTrue(
        r.alias == "myalias",
        r.target.isEmpty,
        r.sourceClubs.isEmpty,
        !r.cumulative,
        !r.focus
      )
    },
    test("empty args gives default alias") {
      val r = RecruitmentApp.parseRecruitArgs(Nil)
      assertTrue(r.alias == "default", r.target.isEmpty)
    },
    test("boolean flags without target") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--cumulative", "--focus"))
      assertTrue(r.cumulative, r.focus, r.target.isEmpty)
    },
    test("target between boolean flags") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--cumulative", "--target", "5", "--focus"))
      assertTrue(r.cumulative, r.focus, r.target.contains(5))
    },
    test("target without value is None") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target"))
      assertTrue(r.target.isEmpty)
    },
    test("target with non-int value is None") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target", "abc"))
      assertTrue(r.target.isEmpty)
    },
    test("flags only, no positional") {
      val r = RecruitmentApp.parseRecruitArgs(List("--target", "20", "--cumulative"))
      assertTrue(r.alias == "default", r.target.contains(20), r.cumulative)
    }
  )

  // ==========================================================================
  // Suite: Criteria helper methods (pure)
  // ==========================================================================

  private def suiteCriteriaHelpers = suite("criteria helpers")(
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
        criteria.dailyMinScoreRate.contains(3.0 / 8),
        criteria.dailyMaxScoreRate.contains(7.0 / 8),
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
  // Suite: Cached criteria (pure), one row per `CacheCriterion`
  // ==========================================================================

  private case class CacheCriterionRow(
    name: String,
    entry: CacheCriterion,
    stalenessDays: Long,
    criteria: RecruitmentCriteria,
    pastLimit: PlayerRecruitmentCache => PlayerRecruitmentCache,
    notPastLimit: PlayerRecruitmentCache => PlayerRecruitmentCache
  )

  private val now: Instant = TestTimes.t0

  private def cacheAged(days: Long): PlayerRecruitmentCache =
    PlayerRecruitmentCache.empty(PlayerId(1), now.minus(days, ChronoUnit.DAYS), None)

  // A timeout from during the run, newer than its row: still inside the window at the limit, so the row's age decides.
  private def duringRun(cache: PlayerRecruitmentCache): Option[Instant] =
    Some(cache.fetchedAt.plusSeconds(1))

  private val onWindowEdge: Instant = now.minus(90, ChronoUnit.DAYS)

  private val justOutsideWindow: Option[Instant] = Some(onWindowEdge.minusSeconds(1))

  private def firingUnder(criteria: RecruitmentCriteria, cache: PlayerRecruitmentCache): List[CacheCriterion] =
    CacheCriterion.values.toList.filter(_.fires(CacheCheck(cache, criteria, now)))

  private def cachedCriteriaTests =
    testEveryCriterionHasARow :: testsTimeoutOnWindowEdgeFires ++ testsRateFiresAtALimitOfZero ++
      cacheCriterionRows.flatMap(cacheCriterionTests)

  private def testEveryCriterionHasARow = test("every cached criterion has a row") {
    assertTrue(CacheCriterion.values.toList.diff(cacheCriterionRows.map(_.entry)).isEmpty)
  }

  private def testsTimeoutOnWindowEdgeFires = List(
    test("zero-tolerance daily timeout: a timeout exactly 90 days old fires") {
      val cache    = cacheAged(0).copy(lastDailyTimeoutAt = Some(onWindowEdge))
      val criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0))
      assertTrue(CacheCriterion.ZeroToleranceDailyTimeout.fires(CacheCheck(cache, criteria, now)))
    },
    test("zero-tolerance team-match timeout: a timeout exactly 90 days old fires") {
      val cache    = cacheAged(0).copy(lastTmTimeoutAt = Some(onWindowEdge))
      val criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0))
      assertTrue(CacheCriterion.ZeroToleranceTmTimeout.fires(CacheCheck(cache, criteria, now)))
    }
  )

  private def testsRateFiresAtALimitOfZero = List(
    test("max daily timeout %: at a limit of 0, a rate above it fires alone once its timeout has left the window") {
      val cache    = cacheAged(0).copy(dailyTimeoutPct = Some(25.0), lastDailyTimeoutAt = justOutsideWindow)
      val criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0))
      assertTrue(firingUnder(criteria, cache) == List(CacheCriterion.MaxDailyTimeoutPercent))
    },
    test(
      "max team-match timeout %: at a limit of 0, a rate above it fires alone once its timeout has left the window"
    ) {
      val cache    = cacheAged(0).copy(tmTimeoutPct90d = Some(25.0), lastTmTimeoutAt = justOutsideWindow)
      val criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0))
      assertTrue(firingUnder(criteria, cache) == List(CacheCriterion.MaxTmTimeoutPercent))
    }
  )

  private def cacheCriterionTests(row: CacheCriterionRow) = {
    def firing(cache: PlayerRecruitmentCache): List[CacheCriterion] = firingUnder(row.criteria, cache)
    List(
      test(
        s"${row.name}: a figure past the limit fires only this criterion until the row is ${row.stalenessDays} days old"
      ) {
        assertTrue(
          firing(row.pastLimit(cacheAged(0))) == List(row.entry),
          firing(row.pastLimit(cacheAged(row.stalenessDays - 1))) == List(row.entry),
          firing(row.pastLimit(cacheAged(row.stalenessDays))).isEmpty
        )
      },
      test(s"${row.name}: a figure not past the limit fires nothing") {
        assertTrue(firing(row.notPastLimit(cacheAged(0))).isEmpty)
      },
      test(s"${row.name}: an absent figure fires nothing") {
        assertTrue(firing(cacheAged(0)).isEmpty)
      }
    )
  }

  private val cacheCriterionRows: List[CacheCriterionRow] = List(
    CacheCriterionRow(
      name = "zero-tolerance daily timeout",
      entry = CacheCriterion.ZeroToleranceDailyTimeout,
      stalenessDays = 90,
      criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0)),
      pastLimit = cache => cache.copy(lastDailyTimeoutAt = duringRun(cache)),
      notPastLimit = _.copy(lastDailyTimeoutAt = justOutsideWindow)
    ),
    CacheCriterionRow(
      name = "zero-tolerance team-match timeout",
      entry = CacheCriterion.ZeroToleranceTmTimeout,
      stalenessDays = 90,
      criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0)),
      pastLimit = cache => cache.copy(lastTmTimeoutAt = duringRun(cache)),
      notPastLimit = _.copy(lastTmTimeoutAt = justOutsideWindow)
    ),
    CacheCriterionRow(
      name = "max clubs",
      entry = CacheCriterion.MaxClubs,
      stalenessDays = 30,
      criteria = makeCriteria().copy(maxClubs = Some(50)),
      pastLimit = _.copy(clubCount = Some(51)),
      notPastLimit = _.copy(clubCount = Some(50))
    ),
    CacheCriterionRow(
      name = "min daily Elo",
      entry = CacheCriterion.MinDailyElo,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinElo = Some(Elo(1000))),
      pastLimit = _.copy(dailyElo = Some(Elo(999))),
      notPastLimit = _.copy(dailyElo = Some(Elo(1000)))
    ),
    CacheCriterionRow(
      name = "max daily Elo",
      entry = CacheCriterion.MaxDailyElo,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxElo = Some(Elo(2000))),
      pastLimit = _.copy(dailyElo = Some(Elo(2001))),
      notPastLimit = _.copy(dailyElo = Some(Elo(2000)))
    ),
    CacheCriterionRow(
      name = "min daily score rate",
      entry = CacheCriterion.MinDailyScoreRate,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinScoreRate = Some(0.5)),
      pastLimit = _.copy(dailyScoreRate = Some(0.49)),
      notPastLimit = _.copy(dailyScoreRate = Some(0.5))
    ),
    CacheCriterionRow(
      name = "max daily score rate",
      entry = CacheCriterion.MaxDailyScoreRate,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxScoreRate = Some(0.8)),
      pastLimit = _.copy(dailyScoreRate = Some(0.81)),
      notPastLimit = _.copy(dailyScoreRate = Some(0.8))
    ),
    CacheCriterionRow(
      name = "max daily timeout %",
      entry = CacheCriterion.MaxDailyTimeoutPercent,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(10.0)),
      pastLimit = _.copy(dailyTimeoutPct = Some(10.5)),
      notPastLimit = _.copy(dailyTimeoutPct = Some(10.0))
    ),
    CacheCriterionRow(
      name = "min daily games finished",
      entry = CacheCriterion.MinDailyGamesFinished,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinGamesFinished = Some(50)),
      pastLimit = _.copy(dailyGamesFinished = Some(49)),
      notPastLimit = _.copy(dailyGamesFinished = Some(50))
    ),
    CacheCriterionRow(
      name = "min ongoing games",
      entry = CacheCriterion.MinOngoingGames,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinOngoingGames = Some(5)),
      pastLimit = _.copy(ongoingGames = Some(4)),
      notPastLimit = _.copy(ongoingGames = Some(5))
    ),
    CacheCriterionRow(
      name = "max ongoing games",
      entry = CacheCriterion.MaxOngoingGames,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxOngoingGames = Some(10)),
      pastLimit = _.copy(ongoingGames = Some(11)),
      notPastLimit = _.copy(ongoingGames = Some(10))
    ),
    CacheCriterionRow(
      name = "min ongoing team matches",
      entry = CacheCriterion.MinOngoingTeamMatches,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinOngoingTeamMatches = Some(2)),
      pastLimit = _.copy(ongoingTeamMatches = Some(1)),
      notPastLimit = _.copy(ongoingTeamMatches = Some(2))
    ),
    CacheCriterionRow(
      name = "min team-match games finished",
      entry = CacheCriterion.MinTmGamesFinished,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinTmGamesFinished = Some(5)),
      pastLimit = _.copy(tmGamesFinished90d = Some(4)),
      notPastLimit = _.copy(tmGamesFinished90d = Some(5))
    ),
    CacheCriterionRow(
      name = "max team-match timeout %",
      entry = CacheCriterion.MaxTmTimeoutPercent,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(10.0)),
      pastLimit = _.copy(tmTimeoutPct90d = Some(10.5)),
      notPastLimit = _.copy(tmTimeoutPct90d = Some(10.0))
    )
  )
}
