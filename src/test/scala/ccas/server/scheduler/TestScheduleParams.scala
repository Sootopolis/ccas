package ccas.server.scheduler

import zio.ZIO
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.subtypes.ClubSlug
import ccas.server.jobs.JobKind
import ccas.server.jobs.JobOptions.{ClubDataOptions, MembershipOptions, RecruitmentOptions}
import ccas.server.scheduler.ScheduleParams.ScheduledJob

/** Decoding a schedule's free-text `params` column into its kind's options and job; what the options mean is
  * [[ccas.server.jobs.TestJobOptions]].
  */
object TestScheduleParams extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestScheduleParams")(
    testAbsentParamsAreDefault,
    testEmptyObjectIsDefault,
    testBlankStringIsDefault,
    testClubDataMinAgeDecodes,
    testRecruitmentFullDecode,
    testMalformedJsonFails,
    testUnknownKeyFails,
    testJobNeedsAClubExactlyForClubKinds,
    testJobRefusesStatsHalfPeriod,
    testJobRefusesRecruitmentZeroTimeLimit,
    testJobRefusesClubDataNegativeMinAge
  )

  // A schedule is the only way ClubData's options are given.
  private def testJobRefusesClubDataNegativeMinAge =
    test("job fails for ClubData params with a negative minAgeHours") {
      ScheduleParams.job(JobKind.ClubData, Some("""{"minAgeHours":-1}""")).either.map { result =>
        assertTrue(result.left.exists(_.getMessage == "'minAgeHours' must be at least 0 (got -1)"))
      }
    }

  private def testJobRefusesRecruitmentZeroTimeLimit =
    test("job fails for Recruitment params with a time limit of 0") {
      ScheduleParams.job(JobKind.Recruitment, Some("""{"timeLimitMinutes":0}""")).either.map { result =>
        assertTrue(result.left.exists(_.getMessage == "'timeLimitMinutes' must be at least 1 (got 0)"))
      }
    }

  private def testJobNeedsAClubExactlyForClubKinds =
    test("job builds a club job for each club-scoped kind, and a clubless one for MatchRef and ClubData") {
      ZIO.foreach(JobKind.values.toList)(kind => ScheduleParams.job(kind, None).map(kind -> _)).map { jobs =>
        val clubless = jobs.collect { case (kind, _: ScheduledJob.Clubless) => kind }.toSet
        assertTrue(clubless == Set(JobKind.MatchRef, JobKind.ClubData))
      }
    }

  private def testJobRefusesStatsHalfPeriod = test("job fails for Stats params with a since but no until") {
    ScheduleParams.job(JobKind.Stats, Some("""{"since":"2026-01-01T00:00:00Z"}""")).exit.map { exit =>
      assertTrue(exit.isFailure)
    }
  }

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

  // A request body pasted whole into params is the likely mistake, now that the two share the options' JSON.
  private def testUnknownKeyFails = test("params with a key no option has fail to decode rather than run on defaults") {
    ScheduleParams.decode(Some("""{"options":{"trustUsernames":false}}"""), MembershipOptions.Default).exit.map { exit =>
      assertTrue(exit.isFailure)
    }
  }
}
