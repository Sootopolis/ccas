package ccas.analysis.apps.recruitment

import ccas.utils.sql.EnumSql

/** The check that rejected a candidate, stored by case name, so renaming a case is a data migration. Most are named
  * after the [[ccas.analysis.tables.RecruitmentCriteria]] field they enforce, the rest after their rule (#430).
  */
enum RejectionReason {
  case AccountNotActive
  case MinDaysSinceRegistration
  case Nationality
  case DaysSinceLastInvited
  case Blacklisted
  case AdminOfSizableClub
  case FormerMember
  case InClubMatch
  case MaxClubs
  case ExcludeClubs
  case AdminOfDiscoveredClub
  case NoDailyStats
  case DailyMinElo
  case DailyMaxElo
  case DailyMinScoreRate
  case DailyMaxScoreRate
  case DailyMaxTimeoutPercent
  case DailyMinGamesFinished
  case DailyMaxHoursPerMove
  case DailyMinOngoingGames
  case DailyMaxOngoingGames
  case DailyMinOngoingTeamMatches
  case DailyMinTmGamesFinished
  case DailyMaxTmTimeoutPercent
  // The cached zero-tolerance checks: a timeout inside the window, not a rate.
  case RecentDailyTimeout
  case RecentTmTimeout
}

object RejectionReason extends EnumSql[RejectionReason]
