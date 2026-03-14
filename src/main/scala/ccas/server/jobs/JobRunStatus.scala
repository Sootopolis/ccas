package ccas.server.jobs

import ccas.utils.sql.EnumSql

enum JobRunStatus {
  case Running, Completed, Failed
}

object JobRunStatus extends EnumSql[JobRunStatus]
