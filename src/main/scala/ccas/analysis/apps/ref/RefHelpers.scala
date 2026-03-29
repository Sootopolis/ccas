package ccas.analysis.apps.ref

import zio.Task
import zio.http.URL

import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch, TeamMatchTeams}
import ccas.api.misc.subtypes.{ClubMatchId, ClubSlug, Username}
import ccas.utils.client.ChessComClient

/** Shared helpers for match-ref resolution, used by RefApp, RecruitmentApp, and Club slug conflict resolution. */
private[analysis] object RefHelpers {

  case class ParsedMatch(matchId: ClubMatchId, isLive: Boolean, matchUrl: URL)

  def parseMatchUrl(atId: URL): ParsedMatch = {
    val matchId  = ClubMatchId.fromUrl(atId)
    val isLive   = atId.path.segments.contains("live")
    val matchUrl = if (isLive) { ApiLiveMatch.getUrl(matchId) } else { ApiDailyMatch.getUrl(matchId) }
    ParsedMatch(matchId, isLive, matchUrl)
  }

  def fetchTeamMatchTeams(
    client: ChessComClient,
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[TeamMatchTeams] =
    if (isLive) { client.get[ApiLiveMatch](ApiLiveMatch.getUrl(matchId)).map(_.teams) }
    else { client.get[ApiDailyMatch](ApiDailyMatch.getUrl(matchId)).map(_.teams) }

  def findPlayerIsTeam1(teams: TeamMatchTeams, username: Username): Option[Boolean] = {
    if (teams.team1.players.exists(_.username == username)) { Some(true) }
    else if (teams.team2.players.exists(_.username == username)) { Some(false) }
    else { None }
  }

  def findClubIsTeam1(teams: TeamMatchTeams, slug: ClubSlug): Option[Boolean] = {
    if (teams.team1.`@id`.path.segments.lastOption.map(ClubSlug.wrap).contains(slug)) { Some(true) }
    else if (teams.team2.`@id`.path.segments.lastOption.map(ClubSlug.wrap).contains(slug)) { Some(false) }
    else { None }
  }
}
