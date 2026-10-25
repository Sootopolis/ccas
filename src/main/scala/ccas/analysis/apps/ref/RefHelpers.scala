package ccas.analysis.apps.ref

import zio.http.URL
import zio.Task

import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch, TeamMatchTeams}
import ccas.api.misc.subtypes.ClubMatchId
import ccas.utils.client.{ChessComClient, FetchResult}

/** Shared helpers for match-ref resolution, used by RefApp, RecruitmentApp, and Club slug conflict resolution. */
private[analysis] object RefHelpers {

  case class ParsedMatch(matchId: ClubMatchId, isLive: Boolean, matchUrl: URL)

  def parseMatchUrl(atId: URL): ParsedMatch = {
    val matchId = ClubMatchId.fromUrl(atId)
    val isLive  = atId.path.segments.contains("live")
    val matchUrl = if (isLive) { ApiLiveMatch.getUrl(matchId) }
    else { ApiDailyMatch.getUrl(matchId) }
    ParsedMatch(matchId, isLive, matchUrl)
  }

  /** Value-based: the daily/live match dispatch, projected down to the shared `TeamMatchTeams` trait, without
    * deciding what absence means. Lets each caller pick its own fold.
    */
  def fetchTeamMatchTeamsResult(
    client: ChessComClient,
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[FetchResult[TeamMatchTeams]] =
    if (isLive) { client.getResult[ApiLiveMatch](ApiLiveMatch.getUrl(matchId)).map(_.map(_.teams)) }
    else { client.getResult[ApiDailyMatch](ApiDailyMatch.getUrl(matchId)).map(_.map(_.teams)) }

  /** `Option`-shaped for callers whose answer to absence is always "give up quietly, try nothing else." */
  def fetchTeamMatchTeamsOptional(
    client: ChessComClient,
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[Option[TeamMatchTeams]] =
    if (isLive) { client.getOptional[ApiLiveMatch](ApiLiveMatch.getUrl(matchId)).map(_.map(_.teams)) }
    else { client.getOptional[ApiDailyMatch](ApiDailyMatch.getUrl(matchId)).map(_.map(_.teams)) }
}
