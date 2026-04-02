package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{Elo, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class PlayerRecruitmentCache(
  playerId: PlayerId,
  fetchedAt: Instant,
  dailyElo: Option[Elo],
  dailyScoreRate: Option[Double],
  dailyTimeoutPct: Option[Double],
  dailyGamesFinished: Option[Int],
  clubCount: Option[Int],
  ongoingGames: Option[Int],
  ongoingTeamMatches: Option[Int],
  tmGamesFinished90d: Option[Int],
  tmTimeoutPct90d: Option[Double],
  lastDailyTimeoutAt: Option[Instant],
  lastTmTimeoutAt: Option[Instant]
) derives DbCodec

object PlayerRecruitmentCache {
  def empty(playerId: PlayerId, fetchedAt: Instant, clubCount: Option[Int]): PlayerRecruitmentCache =
    PlayerRecruitmentCache(
      playerId,
      fetchedAt,
      dailyElo = None,
      dailyScoreRate = None,
      dailyTimeoutPct = None,
      dailyGamesFinished = None,
      clubCount = clubCount,
      ongoingGames = None,
      ongoingTeamMatches = None,
      tmGamesFinished90d = None,
      tmTimeoutPct90d = None,
      lastDailyTimeoutAt = None,
      lastTmTimeoutAt = None
    )

  private val selectCols = SqlLiteral(
    """player_id, fetched_at, daily_elo, daily_score_rate, daily_timeout_pct, daily_games_finished,
       club_count, ongoing_games, ongoing_team_matches, tm_games_finished_90d, tm_timeout_pct_90d,
       last_daily_timeout_at, last_tm_timeout_at"""
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_recruitment_cache (
              player_id              BIGINT PRIMARY KEY REFERENCES player (player_id) ON DELETE RESTRICT,
              fetched_at             TIMESTAMPTZ NOT NULL,
              daily_elo              INT,
              daily_score_rate       DOUBLE PRECISION,
              daily_timeout_pct      DOUBLE PRECISION,
              daily_games_finished   INT,
              club_count             INT,
              ongoing_games          INT,
              ongoing_team_matches   INT,
              tm_games_finished_90d  INT,
              tm_timeout_pct_90d     DOUBLE PRECISION,
              last_daily_timeout_at  TIMESTAMPTZ,
              last_tm_timeout_at     TIMESTAMPTZ
            )""".update.run()
    }

  def selectId(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[PlayerRecruitmentCache]] =
    connectZIO {
      sql"SELECT $selectCols FROM player_recruitment_cache WHERE player_id = $playerId"
        .query[PlayerRecruitmentCache].run().headOption
    }

  def selectTmActive(limit: Int): ZIO[PostgresClient, SQLException, Vector[PlayerRecruitmentCache]] =
    connectZIO {
      sql"""SELECT $selectCols FROM player_recruitment_cache
            WHERE tm_games_finished_90d > 0 OR ongoing_team_matches > 0
            ORDER BY RANDOM() LIMIT $limit"""
        .query[PlayerRecruitmentCache].run()
    }

  def upsert(item: PlayerRecruitmentCache): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_recruitment_cache (
              player_id, fetched_at, daily_elo, daily_score_rate, daily_timeout_pct, daily_games_finished,
              club_count, ongoing_games, ongoing_team_matches, tm_games_finished_90d, tm_timeout_pct_90d,
              last_daily_timeout_at, last_tm_timeout_at
            ) VALUES (
              ${item.playerId}, ${item.fetchedAt}, ${item.dailyElo}, ${item.dailyScoreRate},
              ${item.dailyTimeoutPct}, ${item.dailyGamesFinished},
              ${item.clubCount}, ${item.ongoingGames}, ${item.ongoingTeamMatches},
              ${item.tmGamesFinished90d}, ${item.tmTimeoutPct90d},
              ${item.lastDailyTimeoutAt}, ${item.lastTmTimeoutAt}
            ) ON CONFLICT (player_id) DO UPDATE SET
              fetched_at = EXCLUDED.fetched_at,
              daily_elo = EXCLUDED.daily_elo,
              daily_score_rate = EXCLUDED.daily_score_rate,
              daily_timeout_pct = EXCLUDED.daily_timeout_pct,
              daily_games_finished = EXCLUDED.daily_games_finished,
              club_count = EXCLUDED.club_count,
              ongoing_games = EXCLUDED.ongoing_games,
              ongoing_team_matches = EXCLUDED.ongoing_team_matches,
              tm_games_finished_90d = EXCLUDED.tm_games_finished_90d,
              tm_timeout_pct_90d = EXCLUDED.tm_timeout_pct_90d,
              last_daily_timeout_at = EXCLUDED.last_daily_timeout_at,
              last_tm_timeout_at = EXCLUDED.last_tm_timeout_at""".update.run()
    }

  def deleteAll: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM player_recruitment_cache".update.run()
    }
}
