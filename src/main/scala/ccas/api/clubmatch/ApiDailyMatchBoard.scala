package ccas.api.clubmatch

import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.clubmatch.ApiDailyMatchBoard.ApiDailyBoardGame
import ccas.api.misc.enums.{GameResultDetail, GameRule, TimeClass}
import ccas.api.misc.subtypes.{ClubMatchId, Elo, Username}
import ccas.api.misc.Accuracies
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiDailyMatchBoard(boardScores: Map[Username, Double], games: Chunk[ApiDailyBoardGame])
    derives JsonDecoder {
  require(games.nonEmpty && games.length <= 2, s"A board can only have 1 or 2 games:\n$this")
}

object ApiDailyMatchBoard {
  def getUrl(clubMatchId: ClubMatchId, boardId: Int): URL = ApiDailyMatch.getUrl(clubMatchId).addPath(boardId.toString)

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyBoardGame(
    white: ApiDailyBoardPlayer,
    black: ApiDailyBoardPlayer,
    accuracies: Option[Accuracies],
    url: URL,
    fen: String,
    pgn: String,
    startTime: Long,
    endTime: Option[Long],
    timeControl: String,
    timeClass: TimeClass,
    rules: GameRule,
    rated: Boolean,
    eco: Option[URL],
    `match`: URL
  ) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyBoardPlayer(username: Username, rating: Elo, result: Option[GameResultDetail], `@id`: URL)
      derives JsonDecoder
}
