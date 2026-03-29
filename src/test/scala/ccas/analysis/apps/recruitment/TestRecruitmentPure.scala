package ccas.analysis.apps.recruitment

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.tables.RecruitmentCriteria
import ccas.api.misc.subtypes.ClubSlug

object TestRecruitmentPure extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentPure")(
    suiteParseRecruitArgs,
    suiteCriteriaHelpers
  )

  // ==========================================================================
  // Suite: CLI argument parsing (pure)
  // ==========================================================================

  private def suiteParseRecruitArgs = suite("parseRecruitArgs")(
    test("alias and target from positional + flag") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target", "3"))
      assertTrue(
        r.alias == "default",
        r.target.contains(3),
        r.sourceClubs.isEmpty,
        !r.cumulative,
        !r.focus
      )
    },
    test("source clubs before flags") {
      val r = RecruitmentApp.parseRecruitArgs(List("myalias", "club-a", "club-b", "--target", "10"))
      assertTrue(
        r.alias == "myalias",
        r.sourceClubs.map(ClubSlug.unwrap) == List("club-a", "club-b"),
        r.target.contains(10)
      )
    },
    test("no flags defaults") {
      val r = RecruitmentApp.parseRecruitArgs(List("myalias"))
      assertTrue(
        r.alias == "myalias",
        r.target.isEmpty,
        r.sourceClubs.isEmpty,
        !r.cumulative,
        !r.focus
      )
    },
    test("empty args gives default alias") {
      val r = RecruitmentApp.parseRecruitArgs(Nil)
      assertTrue(r.alias == "default", r.target.isEmpty)
    },
    test("boolean flags without target") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--cumulative", "--focus"))
      assertTrue(r.cumulative, r.focus, r.target.isEmpty)
    },
    test("target between boolean flags") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--cumulative", "--target", "5", "--focus"))
      assertTrue(r.cumulative, r.focus, r.target.contains(5))
    },
    test("target without value is None") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target"))
      assertTrue(r.target.isEmpty)
    },
    test("target with non-int value is None") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target", "abc"))
      assertTrue(r.target.isEmpty)
    },
    test("flags only, no positional") {
      val r = RecruitmentApp.parseRecruitArgs(List("--target", "20", "--cumulative"))
      assertTrue(r.alias == "default", r.target.contains(20), r.cumulative)
    }
  )

  // ==========================================================================
  // Suite: Criteria helper methods (pure)
  // ==========================================================================

  private def suiteCriteriaHelpers = suite("criteria helpers")(
    test("defaultDaily returns expected field values") {
      val criteria = RecruitmentCriteria.defaultDaily
      assertTrue(
        criteria.criteriaId == 0,
        criteria.minDaysSinceRegistration.contains(90),
        criteria.daysSinceLastInvited.contains(180),
        criteria.daysSinceRejected.contains(30),
        !criteria.nationalityExclude,
        criteria.nationalityCountries.isEmpty,
        criteria.excludeClubs.isEmpty,
        criteria.maxClubs.contains(40),
        criteria.excludeSourceAdmins,
        criteria.excludeFormerMembers,
        criteria.dailyMinElo.contains(1000),
        criteria.dailyMaxElo.isEmpty,
        criteria.dailyMinGamesFinished.contains(20),
        criteria.dailyMinTmGamesFinished.contains(10),
        criteria.dailyMaxTimeoutPercent.contains(5.0),
        criteria.dailyMaxTmTimeoutPercent.contains(0.0),
        criteria.dailyMaxHoursPerMove.contains(12),
        criteria.dailyMinOngoingGames.isEmpty,
        criteria.dailyMaxOngoingGames.contains(60),
        criteria.dailyMinOngoingTeamMatches.isEmpty
      )
    }
  )
}
