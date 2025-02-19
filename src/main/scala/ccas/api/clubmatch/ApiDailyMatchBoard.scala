package ccas.api.clubmatch

import ccas.api.clubmatch.ApiDailyMatchBoard.ApiDailyBoardGame
import ccas.api.misc.Accuracies
import ccas.api.misc.enums.{GameResultDetail, GameRule, TimeClass}
import ccas.api.misc.subtypes.{ClubMatchId, Elo, Username}
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
case class ApiDailyMatchBoard(boardScores: Map[Username, Double], games: Chunk[ApiDailyBoardGame]) {
  require(games.nonEmpty && games.length <= 2, s"A board can only have 1 or 2 games:\n$this")
}

object ApiDailyMatchBoard extends JsonDecoding[ApiDailyMatchBoard] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiDailyMatchBoard] = DeriveJsonDecoder.gen

  def getUrl(clubMatchId: ClubMatchId, boardId: Int): URL = ApiDailyMatch.getUrl(clubMatchId).addPath(boardId.toString)

  @jsonMemberNames(SnakeCase)
  case class ApiDailyBoardGame(
    white      : ApiDailyBoardPlayer,
    black      : ApiDailyBoardPlayer,
    accuracies : Option[Accuracies],
    url        : URL,
    fen        : String,
    pgn        : String,
    startTime  : Long,
    endTime    : Option[Long],
    timeControl: String,
    timeClass  : TimeClass,
    rules      : GameRule,
    rated      : Boolean,
    eco        : Option[URL],
    `match`    : URL
  ) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiDailyBoardPlayer(
    username: Username,
    rating  : Elo,
    result  : Option[GameResultDetail],
    `@id`   : URL,
    team    : Option[URL] // TODO if this is never present, remove it
  ) derives JsonDecoder
}
