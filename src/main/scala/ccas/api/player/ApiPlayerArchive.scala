package ccas.api.player

import ccas.api.player.ApiPlayerArchive.ApiPlayerArchiveGame
import ccas.api.misc.Accuracies
import ccas.api.misc.enums.{GameResultDetail, GameRule}
import ccas.api.misc.subtypes.{Elo, Username}
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

import java.time.{Month, Year}
import java.util.UUID

@jsonMemberNames(SnakeCase)
case class ApiPlayerArchive(games: Chunk[ApiPlayerArchiveGame])

object ApiPlayerArchive extends JsonDecoding[ApiPlayerArchive] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerArchive] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  case class ApiPlayerArchiveGame(
    white       : ApiPlayerArchiveGamePlayer,
    black       : ApiPlayerArchiveGamePlayer,
    rated       : Boolean,
    accuracies  : Accuracies,
    URL         : URL,
    fen         : String,
    pgn         : String,
    tcn         : String,
    UUID        : UUID,
    initialSetup: String,
    startTime   : Long,
    endTime     : Long,
    timeControl : String,
    rules       : GameRule,
    eco         : Option[URL],
    tournament  : Option[URL],
    `match`     : Option[URL]
  ) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiPlayerArchiveGamePlayer(
    username: Username,
    `@id`   : URL,
    rating  : Elo,
    uuid    : UUID,
    result  : GameResultDetail
  ) derives JsonDecoder

  def getUrl(username: Username, year: Int, month: Int): URL = {
    require(year > 1970, "year must be greater than 1970")
    require((1 to 12).contains(month), "month must be between 1 and 12")
    val monthString = if (month >= 10) { month.toString } else { s"0$month" }
    ApiPlayerArchives.getUrl(username).addPath(year.toString).addPath(monthString)
  }

  def getUrl(username: Username, year: Year, month: Month): URL = getUrl(username, year.getValue, month.getValue)
}
