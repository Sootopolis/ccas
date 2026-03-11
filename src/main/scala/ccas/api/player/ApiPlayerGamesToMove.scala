package ccas.api.player

import ccas.api.player.ApiPlayerGamesToMove.GameToMove
import ccas.api.misc.subtypes.Username
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
final case class ApiPlayerGamesToMove(games: Chunk[GameToMove])

object ApiPlayerGamesToMove extends JsonDecoding[ApiPlayerGamesToMove] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerGamesToMove] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  final case class GameToMove(url: URL, moveBy: Long, drawOffer: Option[Boolean], lastActivity: Long)
    derives JsonDecoder

  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("to-move")
}
