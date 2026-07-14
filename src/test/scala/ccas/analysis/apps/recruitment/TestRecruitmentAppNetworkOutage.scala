package ccas.analysis.apps.recruitment

import java.time.Instant

import zio.{Exit, Ref, Scope}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.{RecruitmentCandidate, RecruitmentRun, RunTrigger, Tables}
import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.utils.client.{NetworkUnavailableException, TestChessComClientSupport}
import ccas.utils.sql.FreshSchemaLayer

/** A systemic outage during candidate evaluation must abort, not be persisted. `evaluateCandidate`'s first filter
  * (`FetchAndCheckPlayer`) fetches the candidate's profile; under an outage its `catchAll` must re-raise rather than
  * persist a per-candidate `Error` row that would suppress the candidate as evaluated for the alias. Issue #119.
  */
object TestRecruitmentAppNetworkOutage extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentAppNetworkOutage")(
    testEvaluateCandidateOutageReRaises
  ).provideShared(
    FreshSchemaLayer("test_recruitment_app_network_outage", onInit = Tables.ensureTables),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def isNetworkUnavailable(exit: Exit[Throwable, Any]): Boolean =
    exit match {
      case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[NetworkUnavailableException])
      case _                   => false
    }

  private def testEvaluateCandidateOutageReRaises =
    test("outage during candidate evaluation re-raises without persisting an Error row") {
      val criteria = makeCriteria()
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
        client     <- TestChessComClientSupport.networkDownClient
        discovered <- Ref.make(Set.empty[Username])
        failed     <- Ref.make(Set.empty[ClubSlug])
        runCtx = RunContext(
          client,
          criteria,
          clubId,
          "default",
          Set.empty,
          Set.empty,
          Set.empty,
          Set.empty,
          Instant.now(),
          discovered,
          failed
        )
        filters = RecruitmentFilters.buildFilterChain(criteria)
        exit       <- RecruitmentFilters.evaluateCandidate(runId, Username("bob"), runCtx, filters).exit
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(isNetworkUnavailable(exit), candidates.isEmpty)
    }
}
