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
    suite("cached criteria")(cacheCriterionRows.flatMap(cacheCriterionTests)*)
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
  // Suite: Cached criteria (pure), one row per entry in `cacheCriteria`
  // ==========================================================================

  private case class CacheCriterionRow(
    name: String,
    stalenessDays: Long,
    criteria: RecruitmentCriteria,
    pastLimit: PlayerRecruitmentCache => PlayerRecruitmentCache,
    atLimit: PlayerRecruitmentCache => PlayerRecruitmentCache
  )

  private val now: Instant = TestTimes.t0

  private def cacheAged(days: Long): PlayerRecruitmentCache =
    PlayerRecruitmentCache.empty(PlayerId(1), now.minus(days, ChronoUnit.DAYS), None)

  // Slightly older than its row, as a timeout normally is, so it leaves the window as the row reaches the limit.
  private def justBeforeFetch(cache: PlayerRecruitmentCache): Option[Instant] =
    Some(cache.fetchedAt.minusSeconds(1))

  private def cacheCriterionTests(row: CacheCriterionRow) = {
    def rejects(cache: PlayerRecruitmentCache): Boolean =
      RecruitmentFilterDefs.runCacheCriteria(cache, row.criteria, now)
    List(
      test(s"${row.name}: a figure past the limit rejects until the row is ${row.stalenessDays} days old") {
        assertTrue(
          rejects(row.pastLimit(cacheAged(row.stalenessDays - 1))),
          !rejects(row.pastLimit(cacheAged(row.stalenessDays)))
        )
      },
      test(s"${row.name}: a figure at the limit does not reject") {
        assertTrue(!rejects(row.atLimit(cacheAged(0))))
      }
    )
  }

  private val cacheCriterionRows: List[CacheCriterionRow] = List(
    CacheCriterionRow(
      name = "zero-tolerance daily timeout",
      stalenessDays = 90,
      criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0)),
      pastLimit = cache => cache.copy(lastDailyTimeoutAt = justBeforeFetch(cache)),
      atLimit = identity
    ),
    CacheCriterionRow(
      name = "zero-tolerance team-match timeout",
      stalenessDays = 90,
      criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0)),
      pastLimit = cache => cache.copy(lastTmTimeoutAt = justBeforeFetch(cache)),
      atLimit = identity
    ),
    CacheCriterionRow(
      name = "max clubs",
      stalenessDays = 30,
      criteria = makeCriteria().copy(maxClubs = Some(50)),
      pastLimit = _.copy(clubCount = Some(51)),
      atLimit = _.copy(clubCount = Some(50))
    ),
    CacheCriterionRow(
      name = "min daily Elo",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinElo = Some(Elo(1000))),
      pastLimit = _.copy(dailyElo = Some(Elo(999))),
      atLimit = _.copy(dailyElo = Some(Elo(1000)))
    ),
    CacheCriterionRow(
      name = "max daily Elo",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxElo = Some(Elo(2000))),
      pastLimit = _.copy(dailyElo = Some(Elo(2001))),
      atLimit = _.copy(dailyElo = Some(Elo(2000)))
    ),
    CacheCriterionRow(
      name = "min daily score rate",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinScoreRate = Some(0.5)),
      pastLimit = _.copy(dailyScoreRate = Some(0.49)),
      atLimit = _.copy(dailyScoreRate = Some(0.5))
    ),
    CacheCriterionRow(
      name = "max daily score rate",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxScoreRate = Some(0.8)),
      pastLimit = _.copy(dailyScoreRate = Some(0.81)),
      atLimit = _.copy(dailyScoreRate = Some(0.8))
    ),
    CacheCriterionRow(
      name = "max daily timeout %",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(10.0)),
      pastLimit = _.copy(dailyTimeoutPct = Some(10.5)),
      atLimit = _.copy(dailyTimeoutPct = Some(10.0))
    ),
    CacheCriterionRow(
      name = "min daily games finished",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinGamesFinished = Some(50)),
      pastLimit = _.copy(dailyGamesFinished = Some(49)),
      atLimit = _.copy(dailyGamesFinished = Some(50))
    ),
    CacheCriterionRow(
      name = "min ongoing games",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinOngoingGames = Some(5)),
      pastLimit = _.copy(ongoingGames = Some(4)),
      atLimit = _.copy(ongoingGames = Some(5))
    ),
    CacheCriterionRow(
      name = "max ongoing games",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxOngoingGames = Some(10)),
      pastLimit = _.copy(ongoingGames = Some(11)),
      atLimit = _.copy(ongoingGames = Some(10))
    ),
    CacheCriterionRow(
      name = "min ongoing team matches",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinOngoingTeamMatches = Some(2)),
      pastLimit = _.copy(ongoingTeamMatches = Some(1)),
      atLimit = _.copy(ongoingTeamMatches = Some(2))
    ),
    CacheCriterionRow(
      name = "min team-match games finished",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinTmGamesFinished = Some(5)),
      pastLimit = _.copy(tmGamesFinished90d = Some(4)),
      atLimit = _.copy(tmGamesFinished90d = Some(5))
    ),
    CacheCriterionRow(
      name = "max team-match timeout %",
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(10.0)),
      pastLimit = _.copy(tmTimeoutPct90d = Some(10.5)),
      atLimit = _.copy(tmTimeoutPct90d = Some(10.0))
    )
  )
}
