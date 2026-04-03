package ccas.api.clubmatch

import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.clubmatch.ApiMatchBoard.ApiBoardGame
import ccas.api.misc.enums.{GameResultDetail, GameRule, TimeClass}
import ccas.api.misc.subtypes.{ClubMatchId, Elo, Username}
import ccas.api.misc.Accuracies
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiMatchBoard(boardScores: Map[Username, Double], games: Chunk[ApiBoardGame])
    derives JsonDecoder {
  require(games.nonEmpty && games.length <= 2, s"A board can only have 1 or 2 games:\n$this")
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
  ) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiBoardPlayer(username: Username, rating: Elo, result: Option[GameResultDetail], `@id`: URL)
      derives JsonDecoder
}
