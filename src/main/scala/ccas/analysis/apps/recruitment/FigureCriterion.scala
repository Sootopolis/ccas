package ccas.analysis.apps.recruitment

import java.time.Instant
import java.time.temporal.ChronoUnit

import ccas.analysis.tables.{PlayerRecruitmentCache, RecruitmentCriteria, RejectionReason}
import ccas.analysis.tables.PlayerRecruitmentCache.StatsWindowDays

private[recruitment] final case class FigureCheck(
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

/** The filter that fetches a group of figures, writes them to the row and judges them there. */
private[recruitment] enum FigureSource {
  case Clubs, DailyStats, OngoingGames, TmStats
}

/** A check on a row's figures, defined once: the filter named by `liveIn` judges the row it has just written, and
  * [[RecruitmentFilterDefs.CheckCacheCriteria]] judges a stored row while it is younger than `stalenessDays` (#477).
  * The zero-tolerance timeouts judge a timeout's date. The team-match one runs from cache alone, its live rate counting
  * every game; the daily one also runs live, since Chess.com's rate leaves out games its limit counts (#476).
  */
private[recruitment] enum FigureCriterion(
  val reason: RejectionReason,
  private val liveIn: Option[FigureSource],
  private val stalenessDays: Long,
  private val shouldReject: FigureCheck => Boolean
) {

  // These two go by the timeout's own age: the row keeps one after it leaves the window a full evaluation counts. A fresh
  // row keeps its date when a timeout is merged in (#429), so the row's age can close one still inside the window. The
  // rate criteria below also apply at a limit of 0, and by the row's age.
  case ZeroToleranceDailyTimeout
      extends FigureCriterion(
        reason = RejectionReason.RecentDailyTimeout,
        liveIn = Some(FigureSource.DailyStats),
        stalenessDays = StatsWindowDays,
        shouldReject = in =>
          in.criteria.dailyMaxTimeoutPercent.contains(0.0) && in.timeoutInWindow(in.cache.lastDailyTimeoutAt)
      )

  case ZeroToleranceTmTimeout
      extends FigureCriterion(
        reason = RejectionReason.RecentTmTimeout,
        liveIn = None,
        stalenessDays = StatsWindowDays,
        shouldReject = in =>
          in.criteria.dailyMaxTmTimeoutPercent.contains(0.0) && in.timeoutInWindow(in.cache.lastTmTimeoutAt)
      )

  case MaxClubs
      extends FigureCriterion(
        reason = RejectionReason.MaxClubs,
        liveIn = Some(FigureSource.Clubs),
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.maxClubs.exists(max => in.cache.clubCount.exists(_ > max))
      )

  case MinDailyElo
      extends FigureCriterion(
        reason = RejectionReason.DailyMinElo,
        liveIn = Some(FigureSource.DailyStats),
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinElo.exists(min => in.cache.dailyElo.exists(_.value < min.value))
      )

  case MaxDailyElo
      extends FigureCriterion(
        reason = RejectionReason.DailyMaxElo,
        liveIn = Some(FigureSource.DailyStats),
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxElo.exists(max => in.cache.dailyElo.exists(_.value > max.value))
      )

  case MinDailyScoreRate
      extends FigureCriterion(
        reason = RejectionReason.DailyMinScoreRate,
        liveIn = Some(FigureSource.DailyStats),
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinScoreRate.exists(min => in.cache.dailyScoreRate.exists(_ < min))
      )

  case MaxDailyScoreRate
      extends FigureCriterion(
        reason = RejectionReason.DailyMaxScoreRate,
        liveIn = Some(FigureSource.DailyStats),
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxScoreRate.exists(max => in.cache.dailyScoreRate.exists(_ > max))
      )

  case MaxDailyTimeoutPercent
      extends FigureCriterion(
        reason = RejectionReason.DailyMaxTimeoutPercent,
        liveIn = Some(FigureSource.DailyStats),
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxTimeoutPercent.exists(max => in.cache.dailyTimeoutPct.exists(_ > max))
      )

  case MinDailyGamesFinished
      extends FigureCriterion(
        reason = RejectionReason.DailyMinGamesFinished,
        liveIn = Some(FigureSource.DailyStats),
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinGamesFinished.exists(min => in.cache.dailyGamesFinished.exists(_ < min))
      )

  case MinOngoingGames
      extends FigureCriterion(
        reason = RejectionReason.DailyMinOngoingGames,
        liveIn = Some(FigureSource.OngoingGames),
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMinOngoingGames.exists(min => in.cache.ongoingGames.exists(_ < min))
      )

  case MaxOngoingGames
      extends FigureCriterion(
        reason = RejectionReason.DailyMaxOngoingGames,
        liveIn = Some(FigureSource.OngoingGames),
        stalenessDays = FigureStalenessDays,
        shouldReject = in => in.criteria.dailyMaxOngoingGames.exists(max => in.cache.ongoingGames.exists(_ > max))
      )

  case MinOngoingTeamMatches
      extends FigureCriterion(
        reason = RejectionReason.DailyMinOngoingTeamMatches,
        liveIn = Some(FigureSource.OngoingGames),
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinOngoingTeamMatches.exists(min => in.cache.ongoingTeamMatches.exists(_ < min))
      )

  case MinTmGamesFinished
      extends FigureCriterion(
        reason = RejectionReason.DailyMinTmGamesFinished,
        liveIn = Some(FigureSource.TmStats),
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMinTmGamesFinished.exists(min => in.cache.tmGamesFinished90d.exists(_ < min))
      )

  case MaxTmTimeoutPercent
      extends FigureCriterion(
        reason = RejectionReason.DailyMaxTmTimeoutPercent,
        liveIn = Some(FigureSource.TmStats),
        stalenessDays = FigureStalenessDays,
        shouldReject = in =>
          in.criteria.dailyMaxTmTimeoutPercent.exists(max => in.cache.tmTimeoutPct90d.exists(_ > max))
      )

  def judgedLiveBy(source: FigureSource): Boolean = liveIn.contains(source)

  def rejectsFromCache(in: FigureCheck): Boolean = stalenessDays > in.rowAgeDays && shouldReject(in)
}

private[recruitment] object FigureCriterion {

  /** The first criterion `source` judges that the row it has just written fails, whatever the row's age: its figures
    * were fetched just now. The filter's checks on figures no row keeps come after these.
    */
  def firstLive(source: FigureSource, in: FigureCheck): Option[RejectionReason] =
    values.find(criterion => criterion.judgedLiveBy(source) && criterion.shouldReject(in)).map(_.reason)

  def firstFromCache(in: FigureCheck): Option[RejectionReason] =
    values.find(_.rejectsFromCache(in)).map(_.reason)
}
