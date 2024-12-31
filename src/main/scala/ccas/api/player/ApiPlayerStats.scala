package ccas.api.player

import ccas.api.player.ApiPlayerStats.{ApiPlayerDailyStats, ApiPlayerLiveStats}
import ccas.api.utils.enums.GameResult
import ccas.api.utils.subtypes.{Elo, Username}
import ccas.utils.json.JsonDecoding
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiPlayerStats(
  chessDaily   : ApiPlayerDailyStats,
  chess960Daily: ApiPlayerDailyStats,
  chessRapid   : ApiPlayerLiveStats,
  chessBlitz   : ApiPlayerLiveStats,
  chessBullet  : ApiPlayerLiveStats,
)

object ApiPlayerStats extends JsonDecoding[ApiPlayerStats] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerStats] = DeriveJsonDecoder.gen

  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("stats")

  sealed trait ApiPlayerGameTypeStats[Record <: ApiPlayerGameTypeRecord] {
    val last: LatestElo
    val best: BestElo
    val record: Record
    val tournament: Option[TournamentRecord]
  }

  @jsonMemberNames(SnakeCase)
  case class ApiPlayerDailyStats(
    last      : LatestElo,
    best      : BestElo,
    record    : ApiDailyRecord,
    tournament: Option[TournamentRecord]
  ) extends ApiPlayerGameTypeStats[ApiDailyRecord] derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiPlayerLiveStats(
    last      : LatestElo,
    best      : BestElo,
    record    : ApiPlayerLiveRecord,
    tournament: Option[TournamentRecord]
  ) extends ApiPlayerGameTypeStats[ApiPlayerLiveRecord] derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class LatestElo(rating: Elo, date: Instant, rd: Double) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class BestElo(rating: Elo, date: Instant, game: URL) derives JsonDecoder

  sealed trait ApiPlayerGameTypeRecord {
    val win: Int
    val loss: Int
    val draw: Int
    lazy val nGames: Int = win + loss + draw
    lazy val winRate: Double = win / nGames.toDouble
    lazy val scoreRate: Double = // not hardcoding in case chess scoring rules change
      (win * GameResult.Win.score + draw * GameResult.Draw.score + loss * GameResult.Loss.score) / nGames
  }

  @jsonMemberNames(SnakeCase)
  case class ApiPlayerLiveRecord(win: Int, loss: Int, draw: Int) extends ApiPlayerGameTypeRecord derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiDailyRecord(win: Int, loss: Int, draw: Int, timePerMove: Int, timeoutPercent: Double)
    extends ApiPlayerGameTypeRecord derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class TournamentRecord(points: Int, withdraw: Int, count: Int, highestFinish: Int) derives JsonDecoder
}
