package ccas.server.jobs

import java.time.Instant

import zio.ZIO
import zio.test.{assertCompletes, assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.tables.RunTrigger
import ccas.server.jobs.JobOptions.{HistoryOptions, RecruitmentOptions, StatsOptions}

/** The rules a job's options carry, which an HTTP submission and a schedule share. */
object TestJobOptions extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("TestJobOptions")(
    test("a recruitment with no time limit runs for at most JobCaps.MaxTimeLimitMinutes") {
      ZIO.succeed(assertTrue(RecruitmentOptions.Default.cappedTimeLimitMinutes == JobCaps.MaxTimeLimitMinutes))
    },
    test("a recruitment's time limit and target are capped at JobCaps, and smaller ones pass through") {
      val over  = RecruitmentOptions.Default.copy(target = Some(1000), timeLimitMinutes = Some(600))
      val under = RecruitmentOptions.Default.copy(target = Some(5), timeLimitMinutes = Some(10))
      ZIO.succeed(
        assertTrue(
          over.cappedTarget.contains(JobCaps.MaxTarget),
          over.cappedTimeLimitMinutes == JobCaps.MaxTimeLimitMinutes,
          under.cappedTarget.contains(5),
          under.cappedTimeLimitMinutes == 10,
          RecruitmentOptions.Default.cappedTarget.isEmpty
        )
      )
    },
    test("a recruitment target or time limit below 1 is refused before any job is built") {
      def refusal(options: RecruitmentOptions) =
        options.effect(RunTrigger.Api, autoConfirm = true, confirmedCriteriaIdOption = None).flip.map(_.getMessage)
      for {
        zeroTarget     <- refusal(RecruitmentOptions.Default.copy(target = Some(0)))
        negativeTarget <- refusal(RecruitmentOptions.Default.copy(target = Some(-5)))
        zeroLimit      <- refusal(RecruitmentOptions.Default.copy(timeLimitMinutes = Some(0)))
      } yield assertTrue(
        zeroTarget == "'target' must be at least 1 (got 0)",
        negativeTarget == "'target' must be at least 1 (got -5)",
        zeroLimit == "'timeLimitMinutes' must be at least 1 (got 0)"
      )
    },
    test("a recruitment target and time limit of 1 are accepted") {
      RecruitmentOptions.Default
        .copy(target = Some(1), timeLimitMinutes = Some(1))
        .effect(RunTrigger.Api, autoConfirm = true, confirmedCriteriaIdOption = None)
        .as(assertCompletes)
    },
    test("History refresh coalesce: hours wins, else bare flag means 0, else None") {
      val explicitHours = HistoryOptions(None, None, Some(true), Some(5))
      val bareFlag      = HistoryOptions(None, None, Some(true), None)
      ZIO.succeed(
        assertTrue(
          explicitHours.effectiveRefresh.contains(5),
          bareFlag.effectiveRefresh.contains(0),
          HistoryOptions.Default.effectiveRefresh.isEmpty
        )
      )
    },
    test("StatsOptions.period parses both since and until into a period") {
      StatsOptions(Some("2026-01-01"), Some("2026-02-01T00:00:00Z"), None).period.map { period =>
        assertTrue(period.contains((Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-02-01T00:00:00Z"))))
      }
    },
    test("StatsOptions.period with neither since nor until is None (all-time)") {
      StatsOptions.Default.period.map(period => assertTrue(period.isEmpty))
    },
    // The error type is what makes the route answer 400; these pin which input gets which answer.
    test("stats with only one of since and until, or a bad date, are refused before any job is built") {
      for {
        onlySince <- StatsOptions(Some("2026-01-01"), None, None).effect.flip
        badDate   <- StatsOptions(Some("not-a-date"), Some("2026-02-01"), None).effect.flip
      } yield assertTrue(
        onlySince.getMessage == "Both 'since' and 'until' are required for period stats",
        badDate.getMessage.startsWith("Invalid 'since'")
      )
    },
    test("a stats period whose since is not before its until is refused before any job is built") {
      for {
        empty    <- StatsOptions(Some("2026-02-01"), Some("2026-02-01"), None).effect.flip
        reversed <- StatsOptions(Some("2026-03-01"), Some("2026-02-01"), None).effect.flip
      } yield assertTrue(
        empty.getMessage.startsWith("'since' must be before 'until'"),
        reversed.getMessage.startsWith("'since' must be before 'until'")
      )
    }
  )
}
