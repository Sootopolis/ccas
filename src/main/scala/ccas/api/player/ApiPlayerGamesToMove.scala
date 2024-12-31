package ccas.api.player

import ccas.api.player.ApiPlayerGamesToMove.GameToMove
import ccas.api.utils.subtypes.Username
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiPlayerGamesToMove(games: Chunk[GameToMove])

object ApiPlayerGamesToMove extends JsonDecoding[ApiPlayerGamesToMove] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerGamesToMove] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  case class GameToMove(url: URL, moveBy: Instant, drawOffer: Option[Boolean], lastActivity: Instant)
    derives JsonDecoder

  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("to-move")
}
