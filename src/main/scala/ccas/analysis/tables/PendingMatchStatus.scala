package ccas.analysis.tables

import ccas.utils.sql.EnumSql

enum PendingMatchStatus {
  case New          // discovered but not yet attempted
  case ApiError     // fetch from Chess.com API failed
  case Unidentified // match data saved but source club not found in either team
}

object PendingMatchStatus extends EnumSql[PendingMatchStatus]
