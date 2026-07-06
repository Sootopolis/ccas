package ccas.server.jobs

/** Shared upper bounds applied to job parameters regardless of submission path (HTTP `JobRoutes` or a
  * scheduled `JobSchedule`'s params). Kept in one place so the two paths can't drift apart.
  */
object JobCaps {

  /** Cap on a recruitment run's candidate target — bounds API usage per run. */
  val MaxTarget: Int = 40

  /** Cap on a recruitment run's wall-clock time limit (minutes) — keeps individual jobs bounded. */
  val MaxTimeLimitMinutes: Int = 30
}
