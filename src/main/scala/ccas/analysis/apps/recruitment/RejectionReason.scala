package ccas.analysis.apps.recruitment

import ccas.analysis.apps.recruitment.RecruitmentStatsHelpers.StatsWindowDays
import ccas.analysis.apps.recruitment.RejectionReason.Check
import ccas.analysis.apps.recruitment.RejectionReason.Check.{Field, Standing}
import ccas.utils.sql.EnumSql

/** The check that rejected a candidate, stored by case name, so renaming a case is a data migration (#430). A report
  * names it by the criteria field it enforces, which the criteria recap beside it explains (#485).
  */
enum RejectionReason(val check: Check) {
  case AccountNotActive           extends RejectionReason(Standing("account not active"))
  case MinDaysSinceRegistration   extends RejectionReason(Field("minDaysSinceRegistration", None))
  case Nationality                extends RejectionReason(Field("nationalityCountries", None))
  case DaysSinceLastInvited       extends RejectionReason(Field("daysSinceLastInvited", None))
  case Blacklisted                extends RejectionReason(Standing("blacklisted"))
  case AdminOfSizableClub         extends RejectionReason(Field("avoidAdminMinClubSize", Some("on record")))
  case FormerMember               extends RejectionReason(Field("excludeFormerMembers", None))
  case InClubMatch                extends RejectionReason(Standing("in one of the club's matches"))
  case MaxClubs                   extends RejectionReason(Field("maxClubs", None))
  case ExcludeClubs               extends RejectionReason(Field("excludeClubs", None))
  case AdminOfDiscoveredClub      extends RejectionReason(Field("avoidAdminMinClubSize", Some("in their club list")))
  case NoDailyStats               extends RejectionReason(Standing("no daily stats"))
  case DailyMinElo                extends RejectionReason(Field("dailyMinElo", None))
  case DailyMaxElo                extends RejectionReason(Field("dailyMaxElo", None))
  case DailyMinScoreRate          extends RejectionReason(Field("dailyMinScoreRate", None))
  case DailyMaxScoreRate          extends RejectionReason(Field("dailyMaxScoreRate", None))
  case DailyMaxTimeoutPercent     extends RejectionReason(Field("dailyMaxTimeoutPercent", Some("rate")))
  case DailyMinGamesFinished      extends RejectionReason(Field("dailyMinGamesFinished", None))
  case DailyMaxHoursPerMove       extends RejectionReason(Field("dailyMaxHoursPerMove", None))
  case DailyMinOngoingGames       extends RejectionReason(Field("dailyMinOngoingGames", None))
  case DailyMaxOngoingGames       extends RejectionReason(Field("dailyMaxOngoingGames", None))
  case DailyMinOngoingTeamMatches extends RejectionReason(Field("dailyMinOngoingTeamMatches", None))
  case DailyMinTmGamesFinished    extends RejectionReason(Field("dailyMinTmGamesFinished", None))
  case DailyMaxTmTimeoutPercent   extends RejectionReason(Field("dailyMaxTmTimeoutPercent", Some("rate")))
  // The cached zero-tolerance checks: a timeout inside the window, not a rate.
  case RecentDailyTimeout
      extends RejectionReason(Field("dailyMaxTimeoutPercent", Some(s"a timeout in $StatsWindowDays days")))
  case RecentTmTimeout
      extends RejectionReason(Field("dailyMaxTmTimeoutPercent", Some(s"a timeout in $StatsWindowDays days")))

  /** The check as a report names it: its field, qualified where the field has two checks, or what it found. */
  def label: String =
    check match {
      case Field(name, qualifierOption) => qualifierOption.fold(name)(qualifier => s"$name ($qualifier)")
      case Standing(failure)            => failure
    }
}

object RejectionReason extends EnumSql[RejectionReason] {

  enum Check {

    /** Enforces a criteria field, as [[CriteriaSpec]] names it; the qualifier tells apart two checks of one field. */
    case Field(name: String, qualifierOption: Option[String])

    /** Applies whatever the criteria say; `failure` is what the candidate did, in the operator's words. */
    case Standing(failure: String)
  }
}
