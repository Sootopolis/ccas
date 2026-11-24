package ccas.server.jobs

/** Upper bounds on job options, applied by `JobOptions` whichever way the job was submitted. */
object JobCaps {

  /** Cap on a recruitment run's candidate target — bounds API usage per run. */
  val MaxTarget: Int = 40

  /** Cap on a recruitment run's wall-clock time limit (minutes) — keeps individual jobs bounded. */
  val MaxTimeLimitMinutes: Int = 30
}
