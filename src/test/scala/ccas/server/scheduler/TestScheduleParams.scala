package ccas.server.scheduler

import java.time.Instant

import zio.ZIO
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.subtypes.ClubSlug
import ccas.server.scheduler.ScheduleParams.*

/** Unit coverage for decoding a schedule's free-text `params` column into typed per-kind options. This is the
  * load-bearing test for Feature 2: the `runSchedule` threading that consumes these options is straight-line, so
  * correctness of the decode + defaults + validation is what matters.
  */
object TestScheduleParams extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestScheduleParams")(
    testAbsentParamsAreDefault,
    testEmptyObjectIsDefault,
    testBlankStringIsDefault,
    testClubDataMinAgeDecodes,
    testRecruitmentFullDecode,
    testMalformedJsonFails,
    testHistoryEffectiveRefresh,
    testStatsPeriodBothPresent,
    testStatsPeriodNeitherIsNone,
    testStatsPeriodOnlyOneFails
  )

  private def testAbsentParamsAreDefault = test("absent params decode to the kind's all-None Default") {
    ScheduleParams.decode(None, ClubDataOptions.Default).map { opts =>
      assertTrue(opts == ClubDataOptions.Default, opts.minAgeHours.isEmpty)
    }
  }

  private def testEmptyObjectIsDefault = test("empty JSON object decodes to all-None options") {
    ScheduleParams.decode(Some("{}"), RecruitmentOptions.Default).map { opts =>
      assertTrue(opts == RecruitmentOptions.Default)
    }
  }

  private def testBlankStringIsDefault = test("blank params string falls back to Default") {
    ScheduleParams.decode(Some("   "), ClubDataOptions.Default).map { opts =>
      assertTrue(opts == ClubDataOptions.Default)
    }
  }

  private def testClubDataMinAgeDecodes = test("ClubData minAgeHours decodes from JSON") {
    ScheduleParams.decode(Some("""{"minAgeHours":24}"""), ClubDataOptions.Default).map { opts =>
      assertTrue(opts.minAgeHours.contains(24))
    }
  }

  private def testRecruitmentFullDecode = test("Recruitment options decode all fields including source clubs") {
    val json =
      """{"alias":"scouts","target":100,"cumulative":true,"sourceClubs":["club-a","club-b"],"timeLimitMinutes":90,"explore":false}"""
    ScheduleParams.decode(Some(json), RecruitmentOptions.Default).map { opts =>
      assertTrue(
        opts.alias.contains("scouts"),
        opts.target.contains(100),
        opts.cumulative.contains(true),
        opts.sourceClubs.contains(List(ClubSlug("club-a"), ClubSlug("club-b"))),
        opts.timeLimitMinutes.contains(90),
        opts.explore.contains(false)
      )
    }
  }

  private def testMalformedJsonFails = test("malformed params JSON fails to decode") {
    ScheduleParams.decode(Some("{not-json"), ClubDataOptions.Default).exit.map { exit =>
      assertTrue(exit.isFailure)
    }
  }

  private def testHistoryEffectiveRefresh = test("History refresh coalesce: hours wins, else bare flag means 0, else None") {
    val explicitHours = HistoryOptions(None, None, Some(true), Some(5))
    val bareFlag      = HistoryOptions(None, None, Some(true), None)
    val neither       = HistoryOptions(None, None, None, None)
    ZIO.succeed(
      assertTrue(
        HistoryOptions.effectiveRefresh(explicitHours).contains(5),
        HistoryOptions.effectiveRefresh(bareFlag).contains(0),
        HistoryOptions.effectiveRefresh(neither).isEmpty
      )
    )
  }

  private def testStatsPeriodBothPresent = test("statsPeriod parses both since and until into a period") {
    ScheduleParams.statsPeriod(StatsOptions(Some("2026-01-01"), Some("2026-02-01T00:00:00Z"), None)).map { period =>
      assertTrue(
        period.contains((Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-02-01T00:00:00Z")))
      )
    }
  }

  private def testStatsPeriodNeitherIsNone = test("statsPeriod with neither since nor until is None (all-time)") {
    ScheduleParams.statsPeriod(StatsOptions(None, None, None)).map { period =>
      assertTrue(period.isEmpty)
    }
  }

  private def testStatsPeriodOnlyOneFails = test("statsPeriod with only one of since/until fails") {
    ScheduleParams.statsPeriod(StatsOptions(Some("2026-01-01"), None, None)).exit.map { exit =>
      assertTrue(exit.isFailure)
    }
  }
}
