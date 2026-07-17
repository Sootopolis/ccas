package ccas.server.jobs

import ccas.utils.sql.EnumSql

enum JobRunStatus {
  case Running, Completed, Failed, Cancelled
}

object JobRunStatus extends EnumSql[JobRunStatus]
