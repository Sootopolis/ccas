package ccas.analysis.apps.recruitment

import java.time.Instant

import ccas.utils.sql.PostgresClient
import zio.{RIO, Ref}
import zio.http.URL

import ccas.analysis.tables.{PlayerRecruitmentCache, RecruitmentCriteria}
import ccas.api.misc.enums.GameResultDetail
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerClubs, ApiPlayerMatches}
import ccas.api.player.ApiPlayerArchive.ApiPlayerArchiveGame
import ccas.utils.CcasLogger
import ccas.utils.client.ChessComClient
import ccas.utils.ProgressBar

// --- Filter pipeline types ---

private[recruitment] trait RecruitmentFilter {
  def apply(env: FilterEnv): RIO[CcasLogger & PostgresClient, FilterResult]
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
  discoveredClubs: Ref[Set[ClubSlug]],
  discoveredOpponents: Ref[Set[Username]],
  /** Slugs whose `ApiClub.get` failed during this run (e.g. persistent 404s on restricted mega-clubs). The
    * [[ccas.analysis.apps.recruitment.RecruitmentFilterDefs.CheckAdminOfDiscoveredClub]] filter consults this set to
    * avoid re-fetching slugs we've already given up on for this run.
    */
  failedAdminSlugs: Ref[Set[ClubSlug]]
)

/** Accumulated per-candidate state — populated as filters run.
  *
  * @param cacheRejected
  *   Set by [[RecruitmentFilterDefs.CheckCacheCriteria]] when the candidate is rejected purely on cached stats (no
  *   fresh API data fetched beyond the initial player lookup). When true, no
  *   [[ccas.analysis.tables.RecruitmentCandidate]] row is persisted so the candidate is not blocked by the
  *   `daysSinceRejected` cooldown and can be re-evaluated once the cache ages out.
  */
private[recruitment] case class CandidateContext(
  username: Username,
  apiPlayer: Option[ApiPlayer],
  isNewPlayer: Boolean,
  cache: Option[PlayerRecruitmentCache],
  recentArchives: Option[List[ccas.api.player.ApiPlayerArchive]] = None,
  cacheRejected: Boolean = false,
  playerMatches: Option[ApiPlayerMatches] = None,
  playerClubs: Option[ApiPlayerClubs] = None
)
private[recruitment] object CandidateContext {
  def initial(username: Username): CandidateContext =
    CandidateContext(username, apiPlayer = None, isNewPlayer = false, cache = None)
}

/** Groups contexts passed to each filter. */
private[recruitment] case class FilterEnv(run: RunContext, candidate: CandidateContext)

private[recruitment] case class FilterResult(rejected: Boolean, candidate: CandidateContext)

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
  opponentUsernames: Set[Username]
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

// Grim constants (server-side, not user-configurable)
private[recruitment] val GrimConsecutiveRejects = 50

private[recruitment] def isGrim(s: SourceState): Boolean = s.consecutiveRejects >= GrimConsecutiveRejects

// --- ExploreContext: bundles constant parameters across explore loop recursion ---

private[recruitment] case class ExploreContext(
  runId: Long,
  clubSlug: ClubSlug,
  filters: List[RecruitmentFilter],
  runCtx: RunContext,
  invitedRef: Ref[List[Username]],
  evaluatedRef: Ref[Set[Username]],
  evalCountRef: Ref[Int],
  target: Int,
  existingUsernames: Set[Username],
  exploreConcurrency: Int,
  evalChunkSize: Int,
  explore: Boolean,
  showHints: Boolean,
  progressBar: ProgressBar
)

// --- Archive game helpers ---

/** Returns the opponent's username if they did not lose by timeout. */
private[recruitment] def nonTimeoutOpponent(g: ApiPlayerArchiveGame, username: Username): Option[Username] = {
  val isWhite        = g.white.username == username
  val opponentResult = if (isWhite) g.black.result else g.white.result
  val opponentName   = if (isWhite) g.black.username else g.white.username
  Option.when(opponentResult != GameResultDetail.Timeout)(opponentName)
}

/** Returns the player's own result in the game. */
private[recruitment] def playerResult(g: ApiPlayerArchiveGame, username: Username): GameResultDetail =
  if (g.white.username == username) g.white.result else g.black.result
