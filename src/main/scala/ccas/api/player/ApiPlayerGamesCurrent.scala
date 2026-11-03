package ccas.api.player

import zio.Chunk
import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}

import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayerGamesCurrent.ApiPlayerCurrentDailyGame
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiPlayerGamesCurrent(games: Chunk[ApiPlayerCurrentDailyGame]) derives JsonDecoder

object ApiPlayerGamesCurrent {

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerCurrentDailyGame(
    white: URL,
    black: URL,
    url: URL,
    fen: String,
    pgn: String,
    moveBy: Long, // timestamp of when the next move must be made. 0 if the player-to-move is on vacation.
    lastActivity: Long,
    startTime: Long,
    timeControl: String,
    rules: Option[String],
    rated: Boolean,
    tournament: Option[URL],
    `match`: Option[URL]
  ) derives JsonDecoder {
    val whiteUsername: Username = Username.wrap(white.path.segments.last)
    val blackUsername: Username = Username.wrap(black.path.segments.last)

    def isWhite(username: Username): Boolean =
      if (whiteUsername == username) { true }
      else if (blackUsername == username) { false }
      else { throw new IllegalArgumentException(s"Player $username is not present in game $url.") }
  }

  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("games")
}
