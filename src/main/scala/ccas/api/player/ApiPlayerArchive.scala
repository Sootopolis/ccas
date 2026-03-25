package ccas.api.player

import java.time.{Month, Year}
import java.util.UUID

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.enums.{GameResultDetail, GameRule}
import ccas.api.misc.subtypes.{Elo, Username}
import ccas.api.misc.Accuracies
import ccas.api.player.ApiPlayerArchive.ApiPlayerArchiveGame
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiPlayerArchive(games: Chunk[ApiPlayerArchiveGame])

object ApiPlayerArchive extends JsonDecoding[ApiPlayerArchive] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerArchive] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerArchiveGame(
    white: ApiPlayerArchiveGamePlayer,
    black: ApiPlayerArchiveGamePlayer,
    rated: Boolean,
    accuracies: Option[Accuracies],
    URL: URL,
    fen: String,
    pgn: Option[String],
    tcn: String,
    UUID: UUID,
    initialSetup: String,
    startTime: Option[Long],
    endTime: Long,
    timeControl: String,
    rules: GameRule,
    eco: Option[URL],
    tournament: Option[URL],
    `match`: Option[URL],
    timeClass: String
  ) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerArchiveGamePlayer(
    username: Username,
    `@id`: URL,
    rating: Elo,
    uuid: UUID,
    result: GameResultDetail
  ) derives JsonDecoder

  def getUrl(username: Username, year: Int, month: Int): URL = {
    require(year > 1970, "year must be greater than 1970")
    require((1 to 12).contains(month), "month must be between 1 and 12")
    val monthString = f"$month%02d"
    ApiPlayerGamesCurrent.getUrl(username).addPath(year.toString).addPath(monthString)
  }

  def getUrl(username: Username, year: Year, month: Month): URL = getUrl(username, year.getValue, month.getValue)
}
