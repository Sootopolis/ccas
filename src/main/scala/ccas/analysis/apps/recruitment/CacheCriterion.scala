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
  val rowAgeDays: Long = cacheRowAgeDays(cache, now)

  def timeoutInWindow(timeoutAtOption: Option[Instant]): Boolean =
    timeoutAtOption.exists(!_.isBefore(now.minus(StatsWindowDays, ChronoUnit.DAYS)))
}

private def cacheRowAgeDays(cache: PlayerRecruitmentCache, now: Instant): Long =
  ChronoUnit.DAYS.between(cache.fetchedAt, now)

private val FigureStalenessDays = 30L

/** Whether `cache`'s figures can still reject from cache, by its age alone. */
private[recruitment] def figuresFresh(cache: PlayerRecruitmentCache, now: Instant): Boolean =
  FigureStalenessDays > cacheRowAgeDays(cache, now)

/** A check that can reject from cached figures alone. It records its live check's `reason`, except for the two
  * zero-tolerance timeouts, which judge a timeout's date where no live check does.
  */
private[recruitment] enum CacheCriterion(
  val reason: RejectionReason,
  private val stalenessDays: Long,
  private val shouldReject: CacheCheck => Boolean
) {

  // These two go by the timeout's own age: the row keeps one after it leaves the window a full evaluation counts. A fresh
  // row keeps its date when a timeout is merged in (#429), so the row's age can close one still inside the window. The
  // rate criteria below also apply at a limit of 0, and by the row's age.
  case ZeroToleranceDailyTimeout
      extends CacheCriterion(
        reason = RejectionReason.RecentDailyTimeout,
        stalenessDays = StatsWindowDays,
        shouldReject = in =>
          in.criteria.dailyMaxTimeoutPercent.contains(0.0) && in.timeoutInWindow(in.cache.lastDailyTimeoutAt)
      )

  case ZeroToleranceTmTimeout
      extends CacheCriterion(
        reason = RejectionReason.RecentTmTimeout,
        stalenessDays = StatsWindowDays,
        shouldReject = in =>
          in.criteria.dailyMaxTmTimeoutPercent.contains(0.0) && in.timeoutInWindow(in.cache.lastTmTimeoutAt)
      )

  case MaxClubs
      extends CacheCriterion(
        reason = RejectionReason.MaxClubs,
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.maxClubs.exists(max => in.cache.clubCount.exists(_ > max))
      )

  case MinDailyElo
      extends CacheCriterion(
        reason = RejectionReason.DailyMinElo,
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinElo.exists(min => in.cache.dailyElo.exists(_.value < min.value))
      )

  case MaxDailyElo
      extends CacheCriterion(
        reason = RejectionReason.DailyMaxElo,
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxElo.exists(max => in.cache.dailyElo.exists(_.value > max.value))
      )

  case MinDailyScoreRate
      extends CacheCriterion(
        reason = RejectionReason.DailyMinScoreRate,
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinScoreRate.exists(min => in.cache.dailyScoreRate.exists(_ < min))
      )

  case MaxDailyScoreRate
      extends CacheCriterion(
        reason = RejectionReason.DailyMaxScoreRate,
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxScoreRate.exists(max => in.cache.dailyScoreRate.exists(_ > max))
      )

  case MaxDailyTimeoutPercent
      extends CacheCriterion(
        reason = RejectionReason.DailyMaxTimeoutPercent,
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxTimeoutPercent.exists(max => in.cache.dailyTimeoutPct.exists(_ > max))
      )

  case MinDailyGamesFinished
      extends CacheCriterion(
        reason = RejectionReason.DailyMinGamesFinished,
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinGamesFinished.exists(min => in.cache.dailyGamesFinished.exists(_ < min))
      )

  case MinOngoingGames
      extends CacheCriterion(
        reason = RejectionReason.DailyMinOngoingGames,
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinOngoingGames.exists(min => in.cache.ongoingGames.exists(_ < min))
      )

  case MaxOngoingGames
      extends CacheCriterion(
        reason = RejectionReason.DailyMaxOngoingGames,
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxOngoingGames.exists(max => in.cache.ongoingGames.exists(_ > max))
      )

  case MinOngoingTeamMatches
      extends CacheCriterion(
        reason = RejectionReason.DailyMinOngoingTeamMatches,
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinOngoingTeamMatches.exists(min => in.cache.ongoingTeamMatches.exists(_ < min))
      )

  case MinTmGamesFinished
      extends CacheCriterion(
        reason = RejectionReason.DailyMinTmGamesFinished,
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinTmGamesFinished.exists(min => in.cache.tmGamesFinished90d.exists(_ < min))
      )

  case MaxTmTimeoutPercent
      extends CacheCriterion(
        reason = RejectionReason.DailyMaxTmTimeoutPercent,
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMaxTmTimeoutPercent.exists(max => in.cache.tmTimeoutPct90d.exists(_ > max))
      )

  def fires(in: CacheCheck): Boolean = stalenessDays > in.rowAgeDays && shouldReject(in)
}
