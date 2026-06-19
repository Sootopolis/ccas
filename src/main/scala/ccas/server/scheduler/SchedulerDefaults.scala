package ccas.server.scheduler

import com.typesafe.config.{Config, ConfigFactory}

import ccas.server.jobs.JobKind

/** A boot-seed default for one global (all-clubs) maintenance schedule. */
final case class ScheduleSeed(kind: JobKind, intervalHours: Short, enabled: Boolean)

/** Reads the boot-seed defaults for the global server-internal maintenance jobs (`MatchRef`, `ClubData`) from the
  * `scheduler.defaults` HOCON block. Mirrors the inline `ConfigFactory.load()` + `hasPath` pattern in
  * [[JobScheduler.live]]. Values are seed-only: applied once on first boot by [[JobSchedule.seedGlobalIfAbsent]] and
  * thereafter superseded by the live `job_schedule` row.
  */
object SchedulerDefaults {

  // Global maintenance kinds seeded at boot: (kind, HOCON key under scheduler.defaults, default interval hours).
  private val seedKinds: List[(JobKind, String, Short)] = List(
    (JobKind.MatchRef, "matchRef", 24),
    (JobKind.ClubData, "clubData", 6)
  )

  def fromConfig: List[ScheduleSeed] = fromConfig(ConfigFactory.load())

  def fromConfig(cfg: Config): List[ScheduleSeed] =
    seedKinds.map { case (kind, key, defInterval) =>
      val base = s"scheduler.defaults.$key"
      // Read as Int and range-check against SMALLINT bounds before narrowing: a bare `.toShort` would
      // silently wrap an out-of-range value (e.g. 40000 → -25536) past the `> 0` guard.
      val interval =
        if (cfg.hasPath(s"$base.intervalHours")) { cfg.getInt(s"$base.intervalHours") }
        else { defInterval.toInt }
      val enabled =
        if (cfg.hasPath(s"$base.enabled")) { cfg.getBoolean(s"$base.enabled") }
        else { true }
      require(
        interval >= 1 && interval <= Short.MaxValue,
        s"$base.intervalHours must be between 1 and ${Short.MaxValue} (got $interval)"
      )
      ScheduleSeed(kind, interval.toShort, enabled)
    }
}
