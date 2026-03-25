package ccas.analysis.tables

import ccas.utils.sql.EnumSql

// What initiated an app run or job execution.
enum RunTrigger {
  case Cli       // direct CLI execution via ZIOAppDefault
  case Api       // one-off request to the server REST API
  case Scheduled // triggered by the JobScheduler on a recurring schedule
  case FollowUp  // auto-triggered after another job completes (e.g. MatchRef after Recruitment)
}

object RunTrigger extends EnumSql[RunTrigger]
