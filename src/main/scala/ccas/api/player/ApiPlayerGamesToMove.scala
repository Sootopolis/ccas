package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayerGamesToMove.GameToMove
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiPlayerGamesToMove(games: Chunk[GameToMove]) derives JsonDecoder

object ApiPlayerGamesToMove {

  @jsonMemberNames(SnakeCase)
  final case class GameToMove(url: URL, moveBy: Long, drawOffer: Option[Boolean], lastActivity: Long)
      derives JsonDecoder

  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("to-move")
}
