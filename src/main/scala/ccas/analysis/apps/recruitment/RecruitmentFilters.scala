package ccas.analysis.apps.recruitment

import ccas.utils.CcasLogger
import ccas.utils.sql.PostgresClient
import zio.{Clock, RIO, Ref, ZIO}
import RecruitmentFilterDefs.*
import RecruitmentPersistence.*

import ccas.analysis.tables.RecruitmentCriteria
import ccas.api.misc.subtypes.Username
import ccas.utils.errors.safeMessage

private[recruitment] object RecruitmentFilters {

  // --- Public API ---

  def evaluateCandidate(
    runId: Long,
    username: Username,
    runCtx: RunContext,
    filters: List[RecruitmentFilter]
  ): RIO[CcasLogger & PostgresClient, CandidateOutcome] = {
    val candidateCtx = CandidateContext.initial(username)
    Clock.instant.flatMap { now =>
      val env = FilterEnv(runCtx.copy(now = now), candidateCtx)
      def onEvaluationError(ctxRef: Ref[CandidateContext])(error: Throwable): RIO[PostgresClient, CandidateOutcome] =
        ctxRef.get.flatMap { latestCtx =>
          persistCandidateResults(runId, now, latestCtx, CandidateOutcome.Error, env.run.client, Some(error.safeMessage))
        }.as(CandidateOutcome.Error)

      for {
        ctxRef <- Ref.make(candidateCtx)
        result <- (for {
          (outcome, finalCandidate) <- runFilters(env, filters, ctxRef)
          _                         <- persistCandidateResults(runId, now, finalCandidate, outcome, env.run.client)
          _                         <- writePlayerMatchRef(env.run.client, finalCandidate).ignore
        } yield outcome).catchAll(onEvaluationError(ctxRef))
      } yield result
    }
  }

  def buildFilterChain(criteria: RecruitmentCriteria): List[RecruitmentFilter] = {
    val base = List(
      FetchAndCheckPlayer,
      CheckInvitedTooRecently,
      CheckBlacklist
    )
    val adminFilter     = Option.when(criteria.avoidAdminMinClubSize.isDefined)(CheckAdminOfSizableClub)
    val lateAdminFilter = Option.when(criteria.avoidAdminMinClubSize.isDefined)(CheckAdminOfDiscoveredClub)
    val formerMember    = Option.when(criteria.excludeFormerMembers)(CheckFormerMember)
    val rest = List(
      CheckCacheCriteria,
      CheckOpponentMatch,
      CheckClubs
    ) ++ lateAdminFilter ++ List(
      CheckDailyStats,
      CheckOngoingGames
    )
    val teamMatch = Option.when(
      criteria.dailyMinTmGamesFinished.isDefined || criteria.dailyMaxTmTimeoutPercent.isDefined
    )(CheckTmStats)
    base ++ adminFilter ++ formerMember ++ rest ++ teamMatch
  }

  // --- Pipeline runner ---

  private def runFilters(
    env: FilterEnv,
    filters: List[RecruitmentFilter],
    ctxRef: Ref[CandidateContext]
  ): RIO[CcasLogger & PostgresClient, (CandidateOutcome, CandidateContext)] =
    ZIO.foldLeft(filters)(FilterResult(false, env.candidate)) {
      case (r @ FilterResult(true, _), _)     => ZIO.succeed(r)
      case (FilterResult(false, ctx), filter) => ctxRef.set(ctx) *> filter(env.copy(candidate = ctx))
    }.map(r => (if (r.rejected) CandidateOutcome.Rejected else CandidateOutcome.Invited, r.candidate))
}
