package ccas.server.scheduler

import scala.util.Try

import com.typesafe.config.ConfigFactory
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.server.jobs.JobKind

object TestSchedulerDefaults extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestSchedulerDefaults")(
    test("fromConfig returns built-in defaults when the block is absent") {
      // The test classpath application.conf has no scheduler.defaults block, so fallbacks apply.
      val seeds = SchedulerDefaults.fromConfig(ConfigFactory.load())
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
        .withFallback(ConfigFactory.load())
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
        .withFallback(ConfigFactory.load())
      assertTrue(Try(SchedulerDefaults.fromConfig(cfg)).isFailure)
    },
    test("fromConfig rejects an interval above the SMALLINT range") {
      val cfg = ConfigFactory
        .parseString("scheduler.defaults.matchRef.intervalHours = 40000")
        .withFallback(ConfigFactory.load())
      assertTrue(Try(SchedulerDefaults.fromConfig(cfg)).isFailure)
    }
  )
}
