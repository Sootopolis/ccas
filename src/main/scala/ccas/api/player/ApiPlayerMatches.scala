package ccas.api.player

import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch
import ccas.api.misc.enums.GameResultDetail
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
final case class ApiPlayerMatches(
  finished  : Chunk[ApiPlayerMatch],
  inProgress: Chunk[ApiPlayerMatch],
  registered: Chunk[ApiPlayerMatch]
)

object ApiPlayerMatches extends JsonDecoding[ApiPlayerMatches] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerMatches] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerMatch(
    name   : String,
    url    : URL,
    `@id`  : URL,
    club   : URL,
    results: Option[ApiPlayerMatchResults],
    board  : Option[URL]
  ) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerMatchResults(
    playedAsWhite: Option[GameResultDetail],
    playedAsBlack: Option[GameResultDetail]
  ) derives JsonDecoder
}
