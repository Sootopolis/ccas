package ccas.server.scheduler

import com.typesafe.config.{Config, ConfigFactory}

import ccas.server.jobs.JobKind

/** A boot-seed default for one scheduled job — global (all-clubs) or per managed club. */
final case class ScheduleSeed(kind: JobKind, intervalHours: Short, enabled: Boolean)

/** Reads the boot-seed defaults from the `scheduler.defaults` HOCON block: the global server-internal maintenance
  * jobs (`MatchRef`, `ClubData`) via [[fromConfig]], and the per-managed-club jobs (`History`, `Membership`, #102)
  * via [[perClubFromConfig]]. Mirrors the inline `ConfigFactory.load()` + `hasPath` pattern in [[JobScheduler.live]].
  * Values are seed-only: applied once on first boot ([[JobSchedule.seedGlobalIfAbsent]] /
  * [[JobSchedule.seedPerClubIfAbsent]]) and thereafter superseded by the live `job_schedule` row.
  */
object SchedulerDefaults {

  /** One boot-seed entry: which [[JobKind]], its `scheduler.defaults.<hoconKey>` block, and the interval to use when
    * the block is absent.
    */
  private case class SeedDefault(kind: JobKind, hoconKey: String, defaultIntervalHours: Short)

  // Global maintenance kinds seeded at boot.
  private val seedKinds: List[SeedDefault] = List(
    SeedDefault(JobKind.MatchRef, "matchRef", 24),
    SeedDefault(JobKind.ClubData, "clubData", 6)
  )

  // Per-managed-club kinds seeded at boot (#102). Seeded once per managed, non-tombstoned club — never all clubs.
  private val perClubSeedKinds: List[SeedDefault] = List(
    SeedDefault(JobKind.History, "history", 24),
    SeedDefault(JobKind.Membership, "membership", 24)
  )

  def fromConfig: List[ScheduleSeed] = fromConfig(ConfigFactory.load())

  def fromConfig(cfg: Config): List[ScheduleSeed] = seedKinds.map(seedFromKey(cfg, _))

  def perClubFromConfig: List[ScheduleSeed] = perClubFromConfig(ConfigFactory.load())

  def perClubFromConfig(cfg: Config): List[ScheduleSeed] = perClubSeedKinds.map(seedFromKey(cfg, _))

  /** Reads one `scheduler.defaults.<hoconKey>` block into a [[ScheduleSeed]]. `intervalHours` is read as an Int and
    * range-checked against SMALLINT bounds before narrowing: a bare `.toShort` would silently wrap an out-of-range
    * value (e.g. 40000 → -25536) past the `>= 1` guard. `enabled` defaults to true when absent.
    */
  private def seedFromKey(cfg: Config, default: SeedDefault): ScheduleSeed = {
    val base = s"scheduler.defaults.${default.hoconKey}"
    val interval =
      if (cfg.hasPath(s"$base.intervalHours")) { cfg.getInt(s"$base.intervalHours") }
      else { default.defaultIntervalHours.toInt }
    val enabled =
      if (cfg.hasPath(s"$base.enabled")) { cfg.getBoolean(s"$base.enabled") }
      else { true }
    require(
      interval >= 1 && interval <= Short.MaxValue,
      s"$base.intervalHours must be between 1 and ${Short.MaxValue} (got $interval)"
    )
    ScheduleSeed(default.kind, interval.toShort, enabled)
  }
}
