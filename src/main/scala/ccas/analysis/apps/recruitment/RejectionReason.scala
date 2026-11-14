package ccas.analysis.apps.recruitment

import ccas.analysis.apps.recruitment.RecruitmentStatsHelpers.StatsWindowDays
import ccas.utils.sql.EnumSql

/** The check that rejected a candidate, stored by case name, so renaming a case is a data migration (#430).
  *
  * @param criteriaFieldOption
  *   The criteria field the check enforces, as [[CriteriaSpec]] names it to the operator, if it enforces one.
  * @param failure
  *   What the candidate did to fail it, in the operator's words.
  */
enum RejectionReason(val criteriaFieldOption: Option[String], val failure: String) {
  case AccountNotActive         extends RejectionReason(None, "account not active")
  case MinDaysSinceRegistration extends RejectionReason(Some("minDaysSinceRegistration"), "registered too recently")
  case Nationality              extends RejectionReason(Some("nationalityCountries"), "nationality not allowed")
  case DaysSinceLastInvited     extends RejectionReason(Some("daysSinceLastInvited"), "invited too recently")
  case Blacklisted              extends RejectionReason(None, "blacklisted")
  case AdminOfSizableClub
      extends RejectionReason(Some("avoidAdminMinClubSize"), "admin of a sizeable club already on record")
  case FormerMember extends RejectionReason(Some("excludeFormerMembers"), "a former member")
  case InClubMatch  extends RejectionReason(None, "in one of the club's matches")
  case MaxClubs     extends RejectionReason(Some("maxClubs"), "in too many clubs")
  case ExcludeClubs extends RejectionReason(Some("excludeClubs"), "in an excluded club")
  case AdminOfDiscoveredClub
      extends RejectionReason(Some("avoidAdminMinClubSize"), "admin of a sizeable club in their club list")
  case NoDailyStats           extends RejectionReason(None, "no daily stats")
  case DailyMinElo            extends RejectionReason(Some("dailyMinElo"), "daily rating too low")
  case DailyMaxElo            extends RejectionReason(Some("dailyMaxElo"), "daily rating too high")
  case DailyMinScoreRate      extends RejectionReason(Some("dailyMinScoreRate"), "score rate too low")
  case DailyMaxScoreRate      extends RejectionReason(Some("dailyMaxScoreRate"), "score rate too high")
  case DailyMaxTimeoutPercent extends RejectionReason(Some("dailyMaxTimeoutPercent"), "timed out too often")
  case DailyMinGamesFinished
      extends RejectionReason(
        Some("dailyMinGamesFinished"),
        s"too few daily games finished in $StatsWindowDays days"
      )
  case DailyMaxHoursPerMove extends RejectionReason(Some("dailyMaxHoursPerMove"), "moves too slowly")
  case DailyMinOngoingGames extends RejectionReason(Some("dailyMinOngoingGames"), "too few ongoing games")
  case DailyMaxOngoingGames extends RejectionReason(Some("dailyMaxOngoingGames"), "too many ongoing games")
  case DailyMinOngoingTeamMatches
      extends RejectionReason(Some("dailyMinOngoingTeamMatches"), "too few ongoing team-match games")
  case DailyMinTmGamesFinished
      extends RejectionReason(
        Some("dailyMinTmGamesFinished"),
        s"too few team-match games finished in $StatsWindowDays days"
      )
  case DailyMaxTmTimeoutPercent
      extends RejectionReason(Some("dailyMaxTmTimeoutPercent"), "timed out too often in team matches")
  // The cached zero-tolerance checks: a timeout inside the window, not a rate.
  case RecentDailyTimeout
      extends RejectionReason(Some("dailyMaxTimeoutPercent"), s"a timeout in the last $StatsWindowDays days")
  case RecentTmTimeout
      extends RejectionReason(
        Some("dailyMaxTmTimeoutPercent"),
        s"a team-match timeout in the last $StatsWindowDays days"
      )

  /** The check as a report names it: the field it enforces, then what the candidate did. */
  def label: String = criteriaFieldOption.fold(failure)(field => s"$field: $failure")
}

object RejectionReason extends EnumSql[RejectionReason]
