package ccas.api.player

import ccas.api.player.ApiPlayerStats.Stats
import ccas.api.utils.Enums.GameResult
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

  case class LatestElo(rating: Elo, date: Instant, rd: Double)

  case class BestElo(rating: Elo, date: Instant, game: URL)

  case class Record(win: Int, loss: Int, draw: Int, timePerMove: Int, timeoutPercent: Double) {
    val nGames: Int = win + loss + draw
    lazy val winRate: Double = win / nGames.toDouble
    lazy val scoreRate: Double = // not hardcoding in case chess scoring rules change
      (win * GameResult.Win.score + draw * GameResult.Draw.score + loss * GameResult.Loss.score) / nGames
  }

  case class TournamentRecord(points: Int, withdraw: Int, count: Int, highestFinish: Int)
}
