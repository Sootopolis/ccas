package ccas.api.tournament

import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.subtypes.{TournamentSlug, Username}
import ccas.api.misc.Hosts

@jsonMemberNames(SnakeCase)
final case class ApiTournamentRound(
  players: Chunk[ApiTournamentRound.ApiTournamentRoundPlayer]
) derives JsonDecoder

object ApiTournamentRound {
  def getUrl(slug: TournamentSlug, round: Int): URL =
    Hosts.api.addPath("tournament").addPath(slug.value).addPath(round.toString)

  @jsonMemberNames(SnakeCase)
  final case class ApiTournamentRoundPlayer(
    username: Username
  ) derives JsonDecoder
}
