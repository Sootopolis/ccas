package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayerTournaments.ApiPlayerTournament
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiPlayerTournaments(
  finished: Chunk[ApiPlayerTournament],
  inProgress: Chunk[ApiPlayerTournament],
  registered: Chunk[ApiPlayerTournament]
)

object ApiPlayerTournaments extends JsonDecoding[ApiPlayerTournaments] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerTournaments] = DeriveJsonDecoder.gen

  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("tournaments")

  // Union type: fields vary by category (registered has minimal fields, finished has the most).
  // status is a raw String because values span contexts:
  //   finished/in-progress: "eliminated", "active", "winner", "withdrew", "removed"
  //   registration: "invited", "registered"
  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerTournament(
    url: URL,
    `@id`: URL,
    wins: Option[Int],
    losses: Option[Int],
    draws: Option[Int],
    pointsAwarded: Option[Double],
    placement: Option[Int],
    status: String,
    totalPlayers: Option[Int]
  ) derives JsonDecoder
}
