package ccas.api.player

import ccas.api.player.ApiPlayerArchive.ApiPlayerArchiveGame
import ccas.api.utils.Accuracies
import ccas.api.utils.Enums.{GameResultDetail, GameRule}
import ccas.api.utils.Subtypes.{Elo, Username}
import ccas.utils.PrettyPrinting
import zio.Chunk
import zio.http.URL

import java.time.{Instant, Month, Year}
import java.util.UUID

case class ApiPlayerArchive(games: Chunk[ApiPlayerArchiveGame]) extends PrettyPrinting[ApiPlayerArchive]

object ApiPlayerArchive {
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
    startTime   : Instant,
    endTime     : Instant,
    timeControl : String,
    rules       : GameRule,
    eco         : Option[URL],
    tournament  : Option[URL],
    `match`     : Option[URL]
  )

  case class ApiPlayerArchiveGamePlayer(
    username: Username,
    `@id`   : URL,
    rating  : Elo,
    uuid    : UUID,
    result  : GameResultDetail
  )

  def getUrl(username: Username, year: Int, month: Int): URL = {
    require(year > 1970, "year must be greater than 1970")
    require((1 to 12).contains(month), "month must be between 1 and 12")
    val monthString = if (month >= 10) { month.toString } else { s"0$month" }
    ApiPlayerArchives.getUrl(username).addPath(year.toString).addPath(monthString)
  }

  def getUrl(username: Username, year: Year, month: Month): URL = getUrl(username, year.getValue, month.getValue)
}
