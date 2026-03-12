package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayerGamesToMove.GameToMove
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiPlayerGamesToMove(games: Chunk[GameToMove])

object ApiPlayerGamesToMove extends JsonDecoding[ApiPlayerGamesToMove] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerGamesToMove] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  final case class GameToMove(url: URL, moveBy: Long, drawOffer: Option[Boolean], lastActivity: Long)
      derives JsonDecoder

  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("to-move")
}
