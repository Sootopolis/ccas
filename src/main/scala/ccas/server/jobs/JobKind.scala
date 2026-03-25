package ccas.server.jobs

import ccas.utils.sql.EnumSql

enum JobKind {
  case Recruitment, Membership, MatchRef, History
}

object JobKind extends EnumSql[JobKind]
