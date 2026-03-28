package ccas.analysis.tables

import ccas.utils.sql.EnumSql

enum RefSkipReason {
  case NoData            // no matches/tournaments found at all
  case NotFound          // 404 from Chess.com (deleted/renamed account)
  case IdMismatch        // player ID verification failed
  case ResolutionFailed  // had candidates but none resolved
  case ApiError          // transient API error
}

object RefSkipReason extends EnumSql[RefSkipReason]
