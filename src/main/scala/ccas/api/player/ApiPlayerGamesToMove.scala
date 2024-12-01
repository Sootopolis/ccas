package ccas.api.player

import ccas.api.player.ApiPlayerGamesToMove.GameToMove
import ccas.api.utils.Subtypes.Username
import ccas.utils.PrettyPrinting
import zio.Chunk
import zio.http.URL

import java.time.Instant

case class ApiPlayerGamesToMove(games: Chunk[GameToMove]) extends PrettyPrinting[ApiPlayerGamesToMove]

object ApiPlayerGamesToMove {
  case class GameToMove(url: URL, moveBy: Instant, drawOffer: Option[Boolean], lastActivity: Instant)

  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("to-move")
}
