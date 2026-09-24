package ccas.server.scheduler

import scala.util.Try

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.server.jobs.JobKind

object TestSchedulerDefaults extends ZIOSpecDefault {

  // The classpath config resolved without the process environment — for `scheduler.defaults`, which the test file
  // never sets, that is the shipped block. The `wip` worktree's direnv exports SCHEDULER_*_ENABLED, which would
  // otherwise decide what these tests see.
  private val shipped =
    ConfigFactory.load(ConfigParseOptions.defaults(), ConfigResolveOptions.defaults().setUseSystemEnvironment(false))

  override def spec: Spec[Any, Throwable] = suite("TestSchedulerDefaults")(
    test("fromConfig returns the shipped defaults") {
      val seeds = SchedulerDefaults.fromConfig(shipped)
      val byKind = seeds.map(s => s.kind -> s).toMap
      assertTrue(
        seeds.size == 2,
        byKind(JobKind.MatchRef).intervalHours == 24,
        byKind(JobKind.MatchRef).enabled,
        byKind(JobKind.ClubData).intervalHours == 6,
        byKind(JobKind.ClubData).enabled
      )
    },
    test("fromConfig honours overridden interval and enabled") {
      val cfg = ConfigFactory
        .parseString(
          """scheduler.defaults.clubData.intervalHours = 12
            |scheduler.defaults.matchRef.enabled = false
            |""".stripMargin
        )
        .withFallback(shipped)
      val byKind = SchedulerDefaults.fromConfig(cfg).map(s => s.kind -> s).toMap
      assertTrue(
        byKind(JobKind.ClubData).intervalHours == 12,
        !byKind(JobKind.MatchRef).enabled,
        byKind(JobKind.MatchRef).intervalHours == 24
      )
    },
    test("fromConfig rejects a non-positive interval") {
      val cfg = ConfigFactory
        .parseString("scheduler.defaults.matchRef.intervalHours = 0")
        .withFallback(shipped)
      assertTrue(Try(SchedulerDefaults.fromConfig(cfg)).isFailure)
    },
    test("fromConfig rejects an interval above the SMALLINT range") {
      val cfg = ConfigFactory
        .parseString("scheduler.defaults.matchRef.intervalHours = 40000")
        .withFallback(shipped)
      assertTrue(Try(SchedulerDefaults.fromConfig(cfg)).isFailure)
    },
    test("perClubFromConfig returns the shipped defaults") {
      val seeds  = SchedulerDefaults.perClubFromConfig(shipped)
      val byKind = seeds.map(s => s.kind -> s).toMap
      assertTrue(
        seeds.size == 2,
        byKind(JobKind.History).intervalHours == 24,
        byKind(JobKind.History).enabled,
        byKind(JobKind.Membership).intervalHours == 24,
        byKind(JobKind.Membership).enabled
      )
    },
    test("perClubFromConfig honours overridden interval and enabled") {
      val cfg = ConfigFactory
        .parseString(
          """scheduler.defaults.history.intervalHours = 72
            |scheduler.defaults.membership.enabled = false
            |""".stripMargin
        )
        .withFallback(shipped)
      val byKind = SchedulerDefaults.perClubFromConfig(cfg).map(s => s.kind -> s).toMap
      assertTrue(
        byKind(JobKind.History).intervalHours == 72,
        !byKind(JobKind.Membership).enabled,
        byKind(JobKind.Membership).intervalHours == 24
      )
    },
    test("perClubFromConfig rejects a non-positive interval") {
      val cfg = ConfigFactory
        .parseString("scheduler.defaults.history.intervalHours = 0")
        .withFallback(shipped)
      assertTrue(Try(SchedulerDefaults.perClubFromConfig(cfg)).isFailure)
    },
    test("perClubFromConfig rejects an interval above the SMALLINT range") {
      val cfg = ConfigFactory
        .parseString("scheduler.defaults.history.intervalHours = 40000")
        .withFallback(shipped)
      assertTrue(Try(SchedulerDefaults.perClubFromConfig(cfg)).isFailure)
    }
  )
}
