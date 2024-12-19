package ccas.api.clubmatch

import ccas.api.clubmatch.ApiDailyMatchBoard.{ApiDailyBoardGame, BoardScores}
import ccas.api.utils.Accuracies
import ccas.api.utils.Enums.{GameResultDetail, GameRule, TimeClass}
import ccas.api.utils.Subtypes.{ClubMatchId, Elo, Username}
import ccas.utils.PrettyPrinting
import zio.Chunk
import zio.http.URL
import zio.json.{SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiDailyMatchBoard(boardScores: BoardScores, games: Chunk[ApiDailyBoardGame]) {
  require(games.nonEmpty && games.length <= 2, s"A board can only have 1 or 2 games:\n$this")
}

object ApiDailyMatchBoard {
  def getUrl(clubMatchId: ClubMatchId, boardId: Int): URL = ApiDailyMatch.getUrl(clubMatchId).addPath(boardId.toString)

  case class BoardScores(player1: Double, player2: Double)

  case class ApiDailyBoardGame(
    white      : ApiDailyBoardPlayer,
    black      : ApiDailyBoardPlayer,
    accuracies : Option[Accuracies],
    url        : URL,
    fen        : String,
    pgn        : String,
    startTime  : Instant,
    endTime    : Option[Instant],
    timeControl: String,
    timeClass  : TimeClass,
    rules      : GameRule,
    rated      : Boolean,
    eco        : Option[URL],
    `match`    : URL
  )

  case class ApiDailyBoardPlayer(
    username: Username,
    rating  : Elo,
    result  : Option[GameResultDetail],
    `@id`   : URL,
    team    : URL
  )
}
