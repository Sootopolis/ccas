package ccas.api.player

import ccas.api.player.ApiPlayerGamesCurrent.ApiPlayerCurrentDailyGame
import ccas.api.utils.enums.{Colour, GameRule, TimeClass}
import ccas.api.utils.subtypes.Username
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiPlayerGamesCurrent(games: Chunk[ApiPlayerCurrentDailyGame])

object ApiPlayerGamesCurrent extends JsonDecoding[ApiPlayerGamesCurrent] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerGamesCurrent] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
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
  ) derives JsonDecoder {
    val whiteUsername: Username = Username.wrap(white.path.segments.last)
    val blackUsername: Username = Username.wrap(black.path.segments.last)

    def isWhite(username: Username): Boolean = {
      if (whiteUsername == username) { true }
      else if (blackUsername == username) { false }
      else { throw new IllegalArgumentException(s"Player $username is not present in game $url.") }
    }
  }

  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("games")
}
