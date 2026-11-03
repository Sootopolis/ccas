package ccas.analysis.apps.recruitment

import java.time.Instant
import java.time.temporal.ChronoUnit

import RecruitmentStatsHelpers.StatsWindowDays

import ccas.analysis.tables.{PlayerRecruitmentCache, RecruitmentCriteria}

private[recruitment] final case class CacheCheck(
  cache: PlayerRecruitmentCache,
  criteria: RecruitmentCriteria,
  now: Instant
) {
  val rowAgeDays: Long = ChronoUnit.DAYS.between(cache.fetchedAt, now)

  def timeoutInWindow(timeoutAtOption: Option[Instant]): Boolean =
    timeoutAtOption.exists(!_.isBefore(now.minus(StatsWindowDays, ChronoUnit.DAYS)))
}

private val FigureStalenessDays = 30L

private[recruitment] enum CacheCriterion(
  private val stalenessDays: Long,
  private val shouldReject: CacheCheck => Boolean
) {

  // These two go by the timeout's own age: the row keeps one after it leaves the window a full evaluation counts. A
  // timeout is newer than its row only if it came during the run, so the row age rarely decides here. The rate criteria
  // below also apply at a limit of 0, and by the row's age.
  case ZeroToleranceDailyTimeout
      extends CacheCriterion(
        stalenessDays = StatsWindowDays,
        shouldReject = in =>
          in.criteria.dailyMaxTimeoutPercent.contains(0.0) && in.timeoutInWindow(in.cache.lastDailyTimeoutAt)
      )

  case ZeroToleranceTmTimeout
      extends CacheCriterion(
        stalenessDays = StatsWindowDays,
        shouldReject = in =>
          in.criteria.dailyMaxTmTimeoutPercent.contains(0.0) && in.timeoutInWindow(in.cache.lastTmTimeoutAt)
      )

  case MaxClubs
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.maxClubs.exists(max => in.cache.clubCount.exists(_ > max))
      )

  case MinDailyElo
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinElo.exists(min => in.cache.dailyElo.exists(_.value < min.value))
      )

  case MaxDailyElo
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxElo.exists(max => in.cache.dailyElo.exists(_.value > max.value))
      )

  case MinDailyScoreRate
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinScoreRate.exists(min => in.cache.dailyScoreRate.exists(_ < min))
      )

  case MaxDailyScoreRate
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxScoreRate.exists(max => in.cache.dailyScoreRate.exists(_ > max))
      )

  case MaxDailyTimeoutPercent
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxTimeoutPercent.exists(max => in.cache.dailyTimeoutPct.exists(_ > max))
      )

  case MinDailyGamesFinished
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinGamesFinished.exists(min => in.cache.dailyGamesFinished.exists(_ < min))
      )

  case MinOngoingGames
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinOngoingGames.exists(min => in.cache.ongoingGames.exists(_ < min))
      )

  case MaxOngoingGames
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxOngoingGames.exists(max => in.cache.ongoingGames.exists(_ > max))
      )

  case MinOngoingTeamMatches
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinOngoingTeamMatches.exists(min => in.cache.ongoingTeamMatches.exists(_ < min))
      )

  case MinTmGamesFinished
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinTmGamesFinished.exists(min => in.cache.tmGamesFinished90d.exists(_ < min))
      )

  case MaxTmTimeoutPercent
      extends CacheCriterion(
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMaxTmTimeoutPercent.exists(max => in.cache.tmTimeoutPct90d.exists(_ > max))
      )

  def fires(in: CacheCheck): Boolean = stalenessDays > in.rowAgeDays && shouldReject(in)
}
