package ccas.api.clubmatch

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}

import ccas.api.clubmatch.ApiDailyMatch.MatchTeamFinished
import ccas.api.clubmatch.ApiLiveMatch.{ApiLiveMatchSettings, ApiLiveMatchTeams}
import ccas.api.misc.*
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.ClubMatchId
import ccas.utils.json.JsonDecoding

// Only models the finished variant — live matches are transient (minutes, not weeks),
// so registration/in-progress are not useful for identification.
@jsonMemberNames(SnakeCase)
final case class ApiLiveMatch(
  `@id`: URL,
  name: String,
  url: URL,
  startTime: Long,
  endTime: Long,
  status: String,
  boards: Int,
  settings: ApiLiveMatchSettings,
  teams: ApiLiveMatchTeams
)

object ApiLiveMatch extends JsonDecoding[ApiLiveMatch] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiLiveMatch] = DeriveJsonDecoder.gen

  val host: URL = Hosts.api.addPath("match").addPath("live")

  def getUrl(clubMatchId: ClubMatchId): URL = host.addPath(clubMatchId.toString)

  @jsonMemberNames(SnakeCase)
  final case class ApiLiveMatchSettings(
    rules: GameRule,
    timeClass: TimeClass,
    timeControl: String,
    timeIncrement: Option[Int],
    minTeamPlayers: Option[Int],
    minRequiredGames: Int,
    autostart: Option[Boolean]
  ) derives JsonDecoder

  // Team/player structure for finished live matches is identical to daily finished.
  @jsonMemberNames(SnakeCase)
  final case class ApiLiveMatchTeams(
    team1: MatchTeamFinished,
    team2: MatchTeamFinished
  ) extends TeamMatchTeams derives JsonDecoder
}
