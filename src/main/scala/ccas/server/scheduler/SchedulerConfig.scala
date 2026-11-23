package ccas.server.scheduler

import zio.{RIO, ZIO}
import zio.config.magnolia.DeriveConfig

import ccas.utils.config.Settings

/** The `scheduler` section of `application.conf`, where its defaults live. */
final case class SchedulerConfig(pollIntervalMinutes: Int, defaults: SchedulerConfig.Defaults)

object SchedulerConfig {

  /** One `scheduler.defaults` block: the interval a seeded schedule starts with, and whether it starts enabled. */
  final case class Seed(intervalHours: Int, enabled: Boolean)

  /** The boot-seed blocks, one per seeded job kind: [[SchedulerDefaults]]. */
  final case class Defaults(matchRef: Seed, clubData: Seed, history: Seed, membership: Seed)

  given DeriveConfig[Seed]            = DeriveConfig.derived
  given DeriveConfig[Defaults]        = DeriveConfig.derived
  given DeriveConfig[SchedulerConfig] = DeriveConfig.derived

  val load: RIO[Settings, SchedulerConfig] = ZIO.serviceWithZIO[Settings](_.section[SchedulerConfig]("scheduler"))
}
