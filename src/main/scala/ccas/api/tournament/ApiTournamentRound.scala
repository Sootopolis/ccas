package ccas.api.tournament

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.Hosts
import ccas.api.misc.subtypes.{TournamentSlug, Username}
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiTournamentRound(
  players: Chunk[ApiTournamentRound.ApiTournamentRoundPlayer]
)

object ApiTournamentRound extends JsonDecoding[ApiTournamentRound] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiTournamentRound] = DeriveJsonDecoder.gen

  def getUrl(slug: TournamentSlug, round: Int): URL =
    Hosts.api.addPath("tournament").addPath(slug.value).addPath(round.toString)

  @jsonMemberNames(SnakeCase)
  final case class ApiTournamentRoundPlayer(
    username: Username
  ) derives JsonDecoder
}
