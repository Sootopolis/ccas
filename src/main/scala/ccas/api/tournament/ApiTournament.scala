package ccas.api.tournament

import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.Hosts
import ccas.api.misc.subtypes.{TournamentSlug, Username}
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiTournament(
  name: String,
  url: URL,
  status: String,
  players: Chunk[ApiTournament.ApiTournamentPlayer]
) derives JsonDecoder

object ApiTournament {
  def getUrl(slug: TournamentSlug): URL =
    Hosts.api.addPath("tournament").addPath(slug.value)

  @jsonMemberNames(SnakeCase)
  final case class ApiTournamentPlayer(
    username: Username,
    status: String
  ) derives JsonDecoder
}
