package ccas.server.scheduler

import ccas.server.jobs.JobKind

/** A boot-seed default for one scheduled job — global (all-clubs) or per managed club. */
final case class ScheduleSeed(kind: JobKind, intervalHours: Short, enabled: Boolean)

/** The boot-seed defaults in [[SchedulerConfig.defaults]]: the global server-internal maintenance jobs (`MatchRef`,
  * `ClubData`) via [[fromConfig]], and the per-managed-club jobs (`History`, `Membership`, #102) via
  * [[perClubFromConfig]]. Values are seed-only: applied at boot to a schedule that does not exist yet
  * ([[JobSchedule.seedGlobalIfAbsent]] / [[JobSchedule.seedPerClubIfAbsent]]) and thereafter superseded by the live
  * `job_schedule` row.
  */
object SchedulerDefaults {

  def fromConfig(config: SchedulerConfig): List[ScheduleSeed] =
    List(
      seed(JobKind.MatchRef, "matchRef", config.defaults.matchRef),
      seed(JobKind.ClubData, "clubData", config.defaults.clubData)
    )

  // Seeded once per managed club that holds a name — never all clubs.
  def perClubFromConfig(config: SchedulerConfig): List[ScheduleSeed] =
    List(
      seed(JobKind.History, "history", config.defaults.history),
      seed(JobKind.Membership, "membership", config.defaults.membership)
    )

  /** One `scheduler.defaults.<hoconKey>` block as a [[ScheduleSeed]]. `intervalHours` is range-checked against
    * SMALLINT bounds before narrowing: a bare `.toShort` would silently wrap an out-of-range value (e.g. 40000 →
    * -25536) past the `>= 1` guard.
    */
  private def seed(kind: JobKind, hoconKey: String, block: SchedulerConfig.Seed): ScheduleSeed = {
    require(
      block.intervalHours >= 1 && block.intervalHours <= Short.MaxValue,
      s"scheduler.defaults.$hoconKey.intervalHours must be between 1 and ${Short.MaxValue} (got ${block.intervalHours})"
    )
    ScheduleSeed(kind, block.intervalHours.toShort, block.enabled)
  }
}
