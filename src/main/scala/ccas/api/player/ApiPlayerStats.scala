package ccas.api.player

import ccas.api.player.ApiPlayerStats.Stats
import ccas.api.utils.Subtypes.{Elo, Username}
import ccas.utils.PrettyPrinting
import zio.http.URL

import java.time.Instant

case class ApiPlayerStats(
  chessDaily   : Stats,
  chess960Daily: Stats,
  chessRapid   : Stats,
  chessBlitz   : Stats,
  chessBullet  : Stats,
) extends PrettyPrinting[ApiPlayerStats]

object ApiPlayerStats {
  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("stats")

  case class Stats(last: LatestElo, best: BestElo, record: Record, tournament: Option[TournamentRecord])

  case class LatestElo(rating: Elo, date: Instant, rd: Int)

  case class BestElo(rating: Elo, date: Instant, game: URL)

  case class Record(win: Int, loss: Int, draw: Int, timePerMove: Int, timeoutPercent: Int) {
    val nGames: Int = win + loss + draw

    def winRate: Float = win.toFloat / nGames.toFloat

    def scoreRate: Float = (win.toFloat + draw.toFloat / 2f) / nGames.toFloat
  }

  case class TournamentRecord(points: Int, withdraw: Int, count: Int, highestFinish: Int)
}
