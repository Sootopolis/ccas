package ccas.api.player

import ccas.api.player.ApiPlayerGamesToMove.GameToMove
import ccas.api.utils.Subtypes.Username
import zio.Chunk
import zio.http.URL
import zio.json.{SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiPlayerGamesToMove(games: Chunk[GameToMove])

object ApiPlayerGamesToMove {
  case class GameToMove(url: URL, moveBy: Instant, drawOffer: Option[Boolean], lastActivity: Instant)

  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("to-move")
}
