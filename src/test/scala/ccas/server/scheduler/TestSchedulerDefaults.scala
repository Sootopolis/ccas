package ccas.server.scheduler

import scala.util.Try

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import zio.{Task, ZEnvironment, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.server.jobs.JobKind
import ccas.server.scheduler.SchedulerConfig.Seed
import ccas.utils.config.Settings

object TestSchedulerDefaults extends ZIOSpecDefault {

  // The classpath config resolved without the process environment — for `scheduler`, which the test file never sets,
  // that is the shipped block. The `wip` worktree's direnv exports SCHEDULER_*_ENABLED, which would otherwise decide
  // what these tests see.
  private val shipped: Task[SchedulerConfig] =
    ZIO
      .attempt {
        ConfigFactory.load(
          ConfigParseOptions.defaults(),
          ConfigResolveOptions.defaults().setUseSystemEnvironment(false)
        )
      }
      .flatMap(config => SchedulerConfig.load.provideEnvironment(ZEnvironment(Settings.from(config))))

  private def withDefaults(config: SchedulerConfig)(f: SchedulerConfig.Defaults => SchedulerConfig.Defaults) =
    config.copy(defaults = f(config.defaults))

  override def spec: Spec[Any, Throwable] = suite("TestSchedulerDefaults")(
    test("fromConfig returns the shipped defaults") {
      for {
        config <- shipped
        seeds  = SchedulerDefaults.fromConfig(config)
        byKind = seeds.map(s => s.kind -> s).toMap
      } yield assertTrue(
        seeds.size == 2,
        byKind(JobKind.MatchRef).intervalHours == 24,
        byKind(JobKind.MatchRef).enabled,
        byKind(JobKind.ClubData).intervalHours == 6,
        byKind(JobKind.ClubData).enabled
      )
    },
    test("fromConfig honours overridden interval and enabled") {
      for {
        config <- shipped
        overridden = withDefaults(config) { defaults =>
          defaults.copy(clubData = Seed(12, enabled = true), matchRef = defaults.matchRef.copy(enabled = false))
        }
        byKind = SchedulerDefaults.fromConfig(overridden).map(s => s.kind -> s).toMap
      } yield assertTrue(
        byKind(JobKind.ClubData).intervalHours == 12,
        !byKind(JobKind.MatchRef).enabled,
        byKind(JobKind.MatchRef).intervalHours == 24
      )
    },
    test("fromConfig rejects a non-positive interval") {
      for {
        config <- shipped
        zero = withDefaults(config)(_.copy(matchRef = Seed(0, enabled = true)))
      } yield assertTrue(Try(SchedulerDefaults.fromConfig(zero)).isFailure)
    },
    test("fromConfig rejects an interval above the SMALLINT range") {
      for {
        config <- shipped
        tooLarge = withDefaults(config)(_.copy(matchRef = Seed(40000, enabled = true)))
      } yield assertTrue(Try(SchedulerDefaults.fromConfig(tooLarge)).isFailure)
    },
    test("perClubFromConfig returns the shipped defaults") {
      for {
        config <- shipped
        seeds  = SchedulerDefaults.perClubFromConfig(config)
        byKind = seeds.map(s => s.kind -> s).toMap
      } yield assertTrue(
        seeds.size == 2,
        byKind(JobKind.History).intervalHours == 24,
        byKind(JobKind.History).enabled,
        byKind(JobKind.Membership).intervalHours == 24,
        byKind(JobKind.Membership).enabled
      )
    },
    test("perClubFromConfig honours overridden interval and enabled") {
      for {
        config <- shipped
        overridden = withDefaults(config) { defaults =>
          defaults.copy(history = Seed(72, enabled = true), membership = defaults.membership.copy(enabled = false))
        }
        byKind = SchedulerDefaults.perClubFromConfig(overridden).map(s => s.kind -> s).toMap
      } yield assertTrue(
        byKind(JobKind.History).intervalHours == 72,
        !byKind(JobKind.Membership).enabled,
        byKind(JobKind.Membership).intervalHours == 24
      )
    },
    test("perClubFromConfig rejects a non-positive interval") {
      for {
        config <- shipped
        zero = withDefaults(config)(_.copy(history = Seed(0, enabled = true)))
      } yield assertTrue(Try(SchedulerDefaults.perClubFromConfig(zero)).isFailure)
    },
    test("perClubFromConfig rejects an interval above the SMALLINT range") {
      for {
        config <- shipped
        tooLarge = withDefaults(config)(_.copy(history = Seed(40000, enabled = true)))
      } yield assertTrue(Try(SchedulerDefaults.perClubFromConfig(tooLarge)).isFailure)
    }
  )
}
