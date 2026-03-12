package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}

import ccas.api.misc.enums.GameResult
import ccas.api.misc.subtypes.{Elo, Username}
import ccas.api.player.ApiPlayerStats.{ApiPlayerDailyStats, ApiPlayerLiveStats}
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiPlayerStats(
    chessDaily: ApiPlayerDailyStats,
    chess960Daily: ApiPlayerDailyStats,
    chessRapid: ApiPlayerLiveStats,
    chessBlitz: ApiPlayerLiveStats,
    chessBullet: ApiPlayerLiveStats)

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
  final case class ApiPlayerDailyStats(
      last: LatestElo,
      best: BestElo,
      record: ApiDailyRecord,
      tournament: Option[TournamentRecord])
      extends ApiPlayerGameTypeStats[ApiDailyRecord] derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerLiveStats(
      last: LatestElo,
      best: BestElo,
      record: ApiPlayerLiveRecord,
      tournament: Option[TournamentRecord])
      extends ApiPlayerGameTypeStats[ApiPlayerLiveRecord] derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class LatestElo(rating: Elo, date: Long, rd: Double) derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class BestElo(rating: Elo, date: Long, game: URL) derives JsonDecoder

  sealed trait ApiPlayerGameTypeRecord {
    val win: Int
    val loss: Int
    val draw: Int
    lazy val nGames: Int     = win + loss + draw
    lazy val winRate: Double = win / nGames.toDouble
    lazy val scoreRate: Double = // not hardcoding in case chess scoring rules change
      (win * GameResult.Win.score + draw * GameResult.Draw.score + loss * GameResult.Loss.score) / nGames
  }

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerLiveRecord(win: Int, loss: Int, draw: Int) extends ApiPlayerGameTypeRecord
      derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyRecord(win: Int, loss: Int, draw: Int, timePerMove: Int, timeoutPercent: Double)
      extends ApiPlayerGameTypeRecord derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class TournamentRecord(points: Int, withdraw: Int, count: Int, highestFinish: Int) derives JsonDecoder
}
