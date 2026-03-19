package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.PlayerId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class PlayerRecruitmentCache(
  playerId: PlayerId,
  fetchedAt: Instant,
  dailyElo: Option[Int],
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
    """player_id, fetched_at, daily_elo, daily_timeout_pct, daily_games_finished,
       club_count, ongoing_games, ongoing_team_matches, tm_games_finished_90d, tm_timeout_pct_90d,
       last_daily_timeout_at, last_tm_timeout_at"""
  )

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_recruitment_cache (
              player_id              BIGINT PRIMARY KEY,
              fetched_at             TIMESTAMPTZ NOT NULL,
              daily_elo              INT,
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
    } *> connectZIO {
      sql"ALTER TABLE player_recruitment_cache ADD COLUMN IF NOT EXISTS last_daily_timeout_at TIMESTAMPTZ".update.run()
    } *> connectZIO {
      sql"ALTER TABLE player_recruitment_cache ADD COLUMN IF NOT EXISTS last_tm_timeout_at TIMESTAMPTZ".update.run()
    } *> connectZIO {
      sql"ALTER TABLE player_recruitment_cache ALTER COLUMN ongoing_games DROP NOT NULL".update.run()
    } *> connectZIO {
      sql"ALTER TABLE player_recruitment_cache ALTER COLUMN ongoing_team_matches DROP NOT NULL".update.run()
    } *> connectZIO {
      sql"ALTER TABLE player_recruitment_cache ALTER COLUMN tm_games_finished_90d DROP NOT NULL".update.run()
    }

  def selectId(playerId: PlayerId): ZIO[Transactor, SQLException, Option[PlayerRecruitmentCache]] =
    connectZIO {
      sql"SELECT $selectCols FROM player_recruitment_cache WHERE player_id = $playerId"
        .query[PlayerRecruitmentCache].run().headOption
    }

  def upsert(item: PlayerRecruitmentCache): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_recruitment_cache (
              player_id, fetched_at, daily_elo, daily_timeout_pct, daily_games_finished,
              club_count, ongoing_games, ongoing_team_matches, tm_games_finished_90d, tm_timeout_pct_90d,
              last_daily_timeout_at, last_tm_timeout_at
            ) VALUES (
              ${item.playerId}, ${item.fetchedAt}, ${item.dailyElo}, ${item.dailyTimeoutPct}, ${item.dailyGamesFinished},
              ${item.clubCount}, ${item.ongoingGames}, ${item.ongoingTeamMatches},
              ${item.tmGamesFinished90d}, ${item.tmTimeoutPct90d},
              ${item.lastDailyTimeoutAt}, ${item.lastTmTimeoutAt}
            ) ON CONFLICT (player_id) DO UPDATE SET
              fetched_at = EXCLUDED.fetched_at,
              daily_elo = EXCLUDED.daily_elo,
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

  def selectTmActive(limit: Int): ZIO[Transactor, SQLException, Vector[PlayerRecruitmentCache]] =
    connectZIO {
      sql"""SELECT $selectCols FROM player_recruitment_cache
            WHERE tm_games_finished_90d > 0 OR ongoing_team_matches > 0
            ORDER BY RANDOM() LIMIT $limit"""
        .query[PlayerRecruitmentCache].run()
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM player_recruitment_cache".update.run()
    }
}
