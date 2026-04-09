package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.{ClubMatchStatus, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class ClubMatch(
  @Id matchId: ClubMatchId,
  name: String,
  status: ClubMatchStatus,
  timeClass: TimeClass,
  startTime: Option[Instant],
  endTime: Option[Instant],
  boards: Short,
  team1ClubId: Option[ClubId],
  team1ScoreX2: Short,
  team2ClubId: Option[ClubId],
  team2ScoreX2: Short,
  fetchedAt: Instant
) derives DbCodec

object ClubMatch {
  private val repo            = ImmutableRepo[ClubMatch, ClubMatchId]
  private val StaleWindowDays = 90

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_match (
              match_id        BIGINT PRIMARY KEY,
              name            TEXT NOT NULL,
              status          TEXT NOT NULL,
              time_class      TEXT NOT NULL,
              start_time      TIMESTAMPTZ,
              end_time        TIMESTAMPTZ,
              boards          SMALLINT NOT NULL,
              team1_club_id   BIGINT REFERENCES club (club_id) ON DELETE RESTRICT,
              team1_score_x2  SMALLINT NOT NULL,
              team2_club_id   BIGINT REFERENCES club (club_id) ON DELETE RESTRICT,
              team2_score_x2  SMALLINT NOT NULL,
              fetched_at      TIMESTAMPTZ NOT NULL
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_team1_club ON club_match (team1_club_id)""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_team2_club ON club_match (team2_club_id)""".update.run()
    }

  def selectId(matchId: ClubMatchId): ZIO[PostgresClient, SQLException, Option[ClubMatch]] =
    connectZIO(repo.findById(matchId))

  def selectMatchIdsForClub(clubId: ClubId): ZIO[PostgresClient, SQLException, Set[ClubMatchId]] =
    connectZIO {
      sql"""SELECT match_id FROM club_match
            WHERE team1_club_id = $clubId OR team2_club_id = $clubId"""
        .query[ClubMatchId].run().toSet
    }

  def selectStaleForClub(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubMatchId]] =
    connectZIO {
      sql"""SELECT match_id FROM club_match
            WHERE (team1_club_id = $clubId OR team2_club_id = $clubId)
            AND (status != 'Finished' OR fetched_at < end_time + $StaleWindowDays * INTERVAL '1 day')"""
        .query[ClubMatchId].run().toList
    }

  /** Returns match IDs that are finished and were fetched past the stale window (inverse of `selectStaleForClub`).
    * These matches have stable data and don't need re-fetching.
    */
  def selectSettledMatchIdsForClub(clubId: ClubId): ZIO[PostgresClient, SQLException, Set[ClubMatchId]] =
    connectZIO {
      sql"""SELECT match_id FROM club_match
            WHERE (team1_club_id = $clubId OR team2_club_id = $clubId)
            AND status = 'Finished'
            AND fetched_at >= end_time + $StaleWindowDays * INTERVAL '1 day'"""
        .query[ClubMatchId].run().toSet
    }

  def selectClubMatchRef(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[ClubMatchRef]] =
    connectZIO {
      sql"""SELECT match_id, (team1_club_id = $clubId) AS is_team1
            FROM club_match
            WHERE team1_club_id = $clubId OR team2_club_id = $clubId
            LIMIT 1""".query[(ClubMatchId, Boolean)].run().headOption.map { case (matchId, isTeam1) =>
        ClubMatchRef(clubId, matchId, isLive = false, isTeam1)
      }
    }

  def countForClub(clubId: ClubId): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""SELECT COUNT(*) FROM club_match
            WHERE team1_club_id = $clubId OR team2_club_id = $clubId"""
        .query[Long].run().head
    }

  def countForClubInPeriod(clubId: ClubId, since: Instant, until: Instant): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""SELECT COUNT(*) FROM club_match
            WHERE (team1_club_id = $clubId OR team2_club_id = $clubId)
              AND end_time >= $since AND end_time < $until"""
        .query[Long].run().head
    }

  def upsert(item: ClubMatch): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_match (match_id, name, status, time_class, start_time, end_time, boards,
              team1_club_id, team1_score_x2,
              team2_club_id, team2_score_x2, fetched_at)
            VALUES (${item.matchId}, ${item.name}, ${item.status}, ${item.timeClass},
              ${item.startTime}, ${item.endTime}, ${item.boards},
              ${item.team1ClubId}, ${item.team1ScoreX2},
              ${item.team2ClubId}, ${item.team2ScoreX2},
              ${item.fetchedAt})
            ON CONFLICT (match_id) DO UPDATE SET
              name = EXCLUDED.name, status = EXCLUDED.status,
              time_class = EXCLUDED.time_class, start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time,
              boards = EXCLUDED.boards,
              team1_club_id = EXCLUDED.team1_club_id, team1_score_x2 = EXCLUDED.team1_score_x2,
              team2_club_id = EXCLUDED.team2_club_id, team2_score_x2 = EXCLUDED.team2_score_x2,
              fetched_at = EXCLUDED.fetched_at""".update.run()
    }

  /** Counts settled matches (finished + past stale window) with `fetchedAt` before the given cutoff. */
  def countSettledForRefresh(clubId: ClubId, cutoffTime: Instant): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""SELECT COUNT(*) FROM club_match
            WHERE (team1_club_id = $clubId OR team2_club_id = $clubId)
            AND status = 'Finished'
            AND fetched_at >= end_time + $StaleWindowDays * INTERVAL '1 day'
            AND fetched_at < $cutoffTime"""
        .query[Long].run().head
    }

  /** Returns up to `limit` settled match IDs with `fetchedAt` before the given cutoff, ordered by `match_id` for
    * cursor-based pagination. Pass `afterMatchId` from the previous batch's last element to advance the cursor.
    */
  def selectSettledForRefreshBatch(
    clubId: ClubId,
    cutoffTime: Instant,
    limit: Int,
    afterMatchId: ClubMatchId
  ): ZIO[PostgresClient, SQLException, List[ClubMatchId]] =
    connectZIO {
      sql"""SELECT match_id FROM club_match
            WHERE (team1_club_id = $clubId OR team2_club_id = $clubId)
            AND status = 'Finished'
            AND fetched_at >= end_time + $StaleWindowDays * INTERVAL '1 day'
            AND fetched_at < $cutoffTime
            AND match_id > $afterMatchId
            ORDER BY match_id
            LIMIT $limit"""
        .query[ClubMatchId].run().toList
    }

  def updateTeamClubId(matchId: ClubMatchId, isTeam1: Boolean, clubId: ClubId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      if (isTeam1) {
        sql"UPDATE club_match SET team1_club_id = $clubId WHERE match_id = $matchId".update.run()
      } else {
        sql"UPDATE club_match SET team2_club_id = $clubId WHERE match_id = $matchId".update.run()
      }
    }
}
