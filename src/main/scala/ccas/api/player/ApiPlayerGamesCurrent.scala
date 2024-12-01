package ccas.api.player

import ccas.api.utils.Subtypes.Username
import ccas.api.utils.{Colour, GameRule, Subtypes, TimeClass}
import ccas.utils.PrettyPrinting
import zio.Chunk
import zio.http.URL

import java.time.Instant

case class ApiPlayerGamesCurrent(games: Chunk[ApiPlayerGamesCurrent])
  extends PrettyPrinting[ApiPlayerGamesCurrent]

object ApiPlayerGamesCurrent {
  case class ApiPlayerCurrentDailyGame(
    white       : URL,
    black       : URL,
    url         : URL,
    fen         : String,
    pgn         : String,
    turn        : Colour,
    moveBy      : Instant, // timestamp of when the next move must be made. 0 if the player-to-move is on vacation.
    drawOffer   : Option[Colour],
    lastActivity: Instant,
    startTime   : Instant,
    timeControl : String,
    timeClass   : TimeClass,
    rules       : GameRule,
    rated       : Boolean,
    tournament  : Option[URL],
    `match`     : Option[URL]
  ) extends ApiPlayerGame {
    val whiteUsername: Username = Username.wrap(white.path.segments.last)
    val blackUsername: Username = Username.wrap(black.path.segments.last)

    def isWhite(username: Username): Boolean = {
      if (whiteUsername == username) { true }
      else if (blackUsername == username) { false }
      else { throw new IllegalArgumentException("") }
    }
  }

  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("games")
}
