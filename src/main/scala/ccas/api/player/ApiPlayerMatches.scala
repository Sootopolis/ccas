package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.enums.GameResultDetail
import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiPlayerMatches(
  finished: Chunk[ApiPlayerMatch],
  inProgress: Chunk[ApiPlayerMatch],
  registered: Chunk[ApiPlayerMatch]
)

object ApiPlayerMatches extends JsonDecoding[ApiPlayerMatches] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerMatches] = DeriveJsonDecoder.gen

  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("matches")

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerMatch(
    name: String,
    url: URL,
    `@id`: URL,
    club: URL,
    results: Option[ApiPlayerMatchResults],
    board: Option[URL]
  ) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerMatchResults(
    playedAsWhite: Option[GameResultDetail],
    playedAsBlack: Option[GameResultDetail]
  ) derives JsonDecoder
}
