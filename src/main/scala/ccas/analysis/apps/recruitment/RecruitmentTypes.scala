package ccas.analysis.apps.recruitment

import java.time.Instant

import zio.{Ref, RIO}
import zio.http.URL

import ccas.analysis.tables.{PlayerRecruitmentCache, RecruitmentCriteria}
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.enums.GameResultDetail
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerArchive, ApiPlayerClubs, ApiPlayerMatches}
import ccas.api.player.ApiPlayerArchive.ApiPlayerArchiveGame
import ccas.utils.ProgressBar
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

// --- Filter pipeline types ---

private[recruitment] trait RecruitmentFilter {
  def apply(env: FilterEnv): RIO[PostgresClient, FilterResult]

  /** The outcome stored when this filter rejects a candidate. */
  def rejectedAs: CandidateOutcome = CandidateOutcome.Rejected
}

/** Shared across all candidates in a run. */
private[recruitment] case class RunContext(
  client: ChessComClient,
  criteria: RecruitmentCriteria,
  clubId: ClubId,
  alias: String,
  clubMatchIds: Set[URL],
  formerMemberIds: Set[PlayerId],
  adminExcludedPlayerIds: Set[PlayerId],
  excludedSlugs: Set[ClubSlug],
  now: Instant,
  discoveredOpponents: Ref[Set[Username]],
  /** Slugs whose `ApiClub.get` failed during this run (e.g. a Chess.com internal-error 404). The
    * [[ccas.analysis.apps.recruitment.RecruitmentFilterDefs.CheckAdminOfDiscoveredClub]] filter consults this set to
    * avoid re-fetching slugs we've already given up on for this run.
    */
  failedAdminSlugs: Ref[Set[ClubSlug]]
)

/** Accumulated per-candidate state — populated as filters run.
  *
  * @param username
  *   The handle Chess.com last answered to. A filter whose fetch recovers a rename writes the fresh one back, so later
  *   filters and persistence never act on the name the player left.
  */
private[recruitment] case class CandidateContext(
  username: Username,
  apiPlayerOption: Option[ApiPlayer],
  isNewPlayer: Boolean,
  cacheOption: Option[PlayerRecruitmentCache],
  recentArchivesOption: Option[List[ApiPlayerArchive]] = None,
  playerMatchesOption: Option[ApiPlayerMatches] = None,
  playerClubsOption: Option[ApiPlayerClubs] = None
)
private[recruitment] object CandidateContext {
  def initial(username: Username): CandidateContext =
    CandidateContext(username = username, apiPlayerOption = None, isNewPlayer = false, cacheOption = None)
}

/** A candidate who passed every filter. `username` is the handle Chess.com last answered to — after a rename
  * recovery, not the name they were listed under.
  */
private[recruitment] case class FoundCandidate(playerId: PlayerId, username: Username)

/** Groups contexts passed to each filter. */
private[recruitment] case class FilterEnv(run: RunContext, candidate: CandidateContext)

/** `rejectionOption` is the first check this filter's candidate failed, if any. */
private[recruitment] case class FilterResult(rejectionOption: Option[RejectionReason], candidate: CandidateContext)

/** A rejection as it is stored: the rejecting filter's outcome, and the check that failed. */
private[recruitment] case class Rejection(outcome: CandidateOutcome, reason: RejectionReason)

// --- Explore mode types ---

private[recruitment] sealed trait SourceDescriptor {
  val id: String
}

private[recruitment] case class ClubSource(clubSlug: ClubSlug) extends SourceDescriptor {
  val id: String = ClubSlug.unwrap(clubSlug)
}

private[recruitment] case class UsernameSource(id: String, usernames: List[Username]) extends SourceDescriptor

private[recruitment] case class TmStatsResult(
  gamesFinished: Int,
  timeoutPct: Option[Double],
  lastTimeoutAt: Option[Instant],
  opponentUsernames: Set[Username],
  username: Username
)

private[recruitment] case class ActivationResult(
  pool: Map[String, SourceState],
  pending: List[SourceDescriptor],
  visited: Set[ClubSlug]
)

private[recruitment] case class SourceState(
  remaining: List[Username],
  evaluated: Int,
  rejected: Int,
  consecutiveRejects: Int
)

// Grim constants (server-side, not user-configurable). Raised 50 → 100 (2026-06-19): the hardened API client
// (adaptive pacing, response caching, higher fiberCap) makes deeper scans of a source cheap, so we tolerate more
// consecutive rejects before abandoning — trading a bounded amount of extra evaluation for better source coverage.
private[recruitment] val GrimConsecutiveRejects = 100

private[recruitment] def isGrim(s: SourceState): Boolean = s.consecutiveRejects >= GrimConsecutiveRejects

// --- ExploreContext: bundles constant parameters across explore loop recursion ---

private[recruitment] case class ExploreContext(
  runId: RecruitmentRunId,
  clubSlug: ClubSlug,
  filters: List[RecruitmentFilter],
  runCtx: RunContext,
  foundRef: Ref[List[FoundCandidate]],
  evaluatedRef: Ref[Set[Username]],
  evalCountRef: Ref[Int],
  target: Int,
  existingUsernames: Set[Username],
  /** Un-evaluated usernames from grim-abandoned explore sources. `replenish` subtracts these so an abandoned
    * candidate-opponents source is not rediscovered from the (never-cleared) `discoveredOpponents` accumulator.
    */
  abandonedOpponents: Ref[Set[Username]],
  exploreConcurrency: Int,
  evalChunkSize: Int,
  explore: Boolean,
  showHints: Boolean,
  progressBar: ProgressBar
)

// --- Archive game helpers ---

/** Returns the opponent's username if they did not lose by timeout. */
private[recruitment] def nonTimeoutOpponent(g: ApiPlayerArchiveGame, username: Username): Option[Username] = {
  val opponent = g.opponentOf(username)
  Option.when(opponent.result != GameResultDetail.Timeout)(opponent.username)
}
