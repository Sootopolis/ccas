package ccas.analysis.apps.recruitment

import zio.{durationInt, ZIO}
import zio.json.{EncoderOps, JsonDecoder}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubSlug, Elo}
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.FreshSchemaLayer

object TestRecruitmentCriteriaApp extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentCriteriaApp")(
    testSetInsertsCriteriaAndAlias,
    testSetVersioning,
    testSetDedupUnchanged,
    testSetUnknownClub,
    testSetRejectsInvalid,
    testSetRejectsLongAlias,
    testValidateRejectsBadRanges,
    testDiffLinesNewAndChanges,
    testCriteriaSpecRoundTrip
  ).provideShared(
    FreshSchemaLayer("test_recruitment_criteria", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def testSetInsertsCriteriaAndAlias = test("set inserts criteria and a newest-wins alias row") {
    val criteria = makeCriteria(excludeFormerMembers = true)
    for {
      _        <- seedDb
      id       <- RecruitmentCriteriaApp.set(clubSlug, "default", criteria)
      aliasRow <- RecruitmentAlias.selectLatest(clubId, "default")
      stored   <- RecruitmentCriteria.selectId(id)
    } yield assertTrue(
      aliasRow.exists(_.criteriaId == id),
      stored.exists(_.criteriaId == id),
      stored.exists(_.excludeFormerMembers)
    )
  }

  private def testSetVersioning =
    test("second set creates a new criteria row; alias resolves to the newest") {
      for {
        _        <- seedDb
        id1      <- RecruitmentCriteriaApp.set(clubSlug, "default", makeCriteria(daysSinceRejected = Some(10)))
        _        <- ZIO.sleep(5.millis) // distinct `since` so the composite PK doesn't collide
        id2      <- RecruitmentCriteriaApp.set(clubSlug, "default", makeCriteria(daysSinceRejected = Some(20)))
        latest   <- RecruitmentAlias.selectLatest(clubId, "default")
        distinct <- RecruitmentAlias.countDistinct(clubId)
        criteria <- ZIO.foreach(latest)(row => RecruitmentCriteria.selectId(row.criteriaId)).map(_.flatten)
      } yield assertTrue(
        id1 != id2,
        latest.exists(_.criteriaId == id2),
        distinct == 1,
        criteria.exists(_.daysSinceRejected.contains(20))
      )
    }

  private def testSetDedupUnchanged =
    test("set with unchanged criteria reuses the existing version, no new row") {
      val criteria = makeCriteria(daysSinceRejected = Some(30))
      for {
        _        <- seedDb
        id1      <- RecruitmentCriteriaApp.set(clubSlug, "default", criteria)
        _        <- ZIO.sleep(5.millis)
        id2      <- RecruitmentCriteriaApp.set(clubSlug, "default", criteria)
        distinct <- RecruitmentAlias.countDistinct(clubId)
      } yield assertTrue(id1 == id2, distinct == 1)
    }

  private def testSetUnknownClub = test("set on an unknown club fails with NotFoundException") {
    for {
      _      <- seedDb
      result <- RecruitmentCriteriaApp.set(ClubSlug("no-such-club"), "default", makeCriteria()).either
    } yield assertTrue(result.left.exists(_.isInstanceOf[NotFoundException]))
  }

  private def testSetRejectsInvalid = test("set rejects invalid criteria with BadRequestException") {
    val bad = makeCriteria().copy(dailyMinScoreRate = Some(2.0))
    for {
      _      <- seedDb
      result <- RecruitmentCriteriaApp.set(clubSlug, "default", bad).either
    } yield assertTrue(result.left.exists(_.isInstanceOf[BadRequestException]))
  }

  private def testValidateRejectsBadRanges = test("validate rejects inverted and out-of-range fields") {
    val badElo     = makeCriteria().copy(dailyMinElo = Some(Elo(2000)), dailyMaxElo = Some(Elo(1000)))
    val badRate    = makeCriteria().copy(dailyMinScoreRate = Some(1.5))
    val badOngoing = makeCriteria().copy(dailyMinOngoingGames = Some(10), dailyMaxOngoingGames = Some(5))
    val good       = makeCriteria().copy(dailyMinElo = Some(Elo(1000)), dailyMaxElo = Some(Elo(2000)))
    assertTrue(
      RecruitmentCriteriaApp.validate(badElo).isLeft,
      RecruitmentCriteriaApp.validate(badRate).isLeft,
      RecruitmentCriteriaApp.validate(badOngoing).isLeft,
      RecruitmentCriteriaApp.validate(good).isRight
    )
  }

  private def testSetRejectsLongAlias = test("set rejects an alias longer than MaxAliasLength") {
    val tooLong = "x" * (RecruitmentCriteriaApp.MaxAliasLength + 1)
    for {
      _      <- seedDb
      result <- RecruitmentCriteriaApp.set(clubSlug, tooLong, makeCriteria()).either
    } yield assertTrue(result.left.exists(_.isInstanceOf[BadRequestException]))
  }

  private def testDiffLinesNewAndChanges = test("diffLines: (new alias) when no prior; labeled lines when changed") {
    val before = makeCriteria(daysSinceRejected = Some(10), excludeFormerMembers = false)
    val after  = makeCriteria(daysSinceRejected = Some(20), excludeFormerMembers = true)
    val newLines     = RecruitmentCriteriaApp.diffLines(None, after)
    val changeLines  = RecruitmentCriteriaApp.diffLines(Some(before), after)
    val unchanged    = RecruitmentCriteriaApp.diffLines(Some(after), after)
    assertTrue(
      newLines == List("(new alias)"),
      changeLines.exists(_.startsWith("daysSinceRejected: 10 → 20")),
      changeLines.exists(_.startsWith("excludeFormerMembers: false → true")),
      changeLines.size == 2,
      unchanged.isEmpty
    )
  }

  private def testCriteriaSpecRoundTrip = test("CriteriaSpec round-trips JSON and is identity over RecruitmentCriteria") {
    val criteria = RecruitmentCriteria.defaultDaily
    val spec     = CriteriaSpec.fromCriteria(criteria)
    val decoded  = JsonDecoder[CriteriaSpec].decodeJson(spec.toJson)
    assertTrue(
      decoded == Right(spec),
      spec.toCriteria == criteria
    )
  }
}
