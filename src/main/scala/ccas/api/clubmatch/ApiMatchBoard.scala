package ccas.api.clubmatch

import zio.Chunk
import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}

import ccas.api.clubmatch.ApiMatchBoard.ApiBoardGame
import ccas.api.misc.Accuracies
import ccas.api.misc.enums.{GameResultDetail, GameRule, TimeClass}
import ccas.api.misc.subtypes.{ClubMatchId, Elo, Username}
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiMatchBoard(boardScores: Map[Username, Double], games: Chunk[ApiBoardGame]) derives JsonDecoder {
  require(games.nonEmpty && games.length <= 2, s"A board can only have 1 or 2 games:\n$this")

  /** Every player on the board, once each. */
  def usernames: List[Username] = games.toList.flatMap(_.usernames).distinct
}

object ApiMatchBoard {
  def dailyUrl(clubMatchId: ClubMatchId, boardId: Int): URL =
    ApiDailyMatch.getUrl(clubMatchId).addPath(boardId.toString)

  def liveUrl(clubMatchId: ClubMatchId, boardId: Int): URL =
    ApiLiveMatch.getUrl(clubMatchId).addPath(boardId.toString)

  private given JsonDecoder[Either[URL, ApiBoardPlayer]] =
    JsonDecoder[URL].orElseEither(JsonDecoder[ApiBoardPlayer])

  @jsonMemberNames(SnakeCase)
  final case class ApiBoardGame(
    white: Either[URL, ApiBoardPlayer],
    black: Either[URL, ApiBoardPlayer],
    accuracies: Option[Accuracies],
    url: URL,
    fen: String,
    pgn: Option[String],
    startTime: Option[Long],
    endTime: Option[Long],
    timeControl: String,
    timeClass: TimeClass,
    rules: GameRule,
    rated: Boolean,
    eco: Option[URL],
    `match`: Option[URL]
  ) derives JsonDecoder {

    /** Both sides' usernames. A side that decodes as a bare URL is named by its last path segment, and dropped for a
      * closed-account placeholder with an empty path (defensive against Chess.com edge cases — every observed URL has
      * a `/pub/player/<name>` shape, but a caller should not throw if the API ever ships an unusual response).
      */
    def usernames: List[Username] = List(white, black).flatMap {
      case Right(player) => Some(player.username)
      case Left(url)     => url.path.segments.lastOption.map(Username.wrap)
    }
  }

  @jsonMemberNames(SnakeCase)
  final case class ApiBoardPlayer(username: Username, rating: Elo, result: Option[GameResultDetail], `@id`: URL)
      derives JsonDecoder
}
