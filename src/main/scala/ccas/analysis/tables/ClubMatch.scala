package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.{ClubMatchResult, ClubMatchStatus, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class ClubMatch(
  @Id matchId: ClubMatchId,
  name: String,
  url: String,
  status: ClubMatchStatus,
  timeClass: TimeClass,
  startTime: Option[Instant],
  endTime: Option[Instant],
  boards: Int,
  team1ClubId: Option[ClubId],
  team1Name: String,
  team1Score: Double,
  team1Result: Option[ClubMatchResult],
  team2ClubId: Option[ClubId],
  team2Name: String,
  team2Score: Double,
  team2Result: Option[ClubMatchResult],
  fetchedAt: Instant
) derives DbCodec

object ClubMatch {
  private val repo = ImmutableRepo[ClubMatch, ClubMatchId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_match (
              match_id       BIGINT PRIMARY KEY,
              name           VARCHAR NOT NULL,
              url            VARCHAR NOT NULL,
              status         VARCHAR NOT NULL,
              time_class     VARCHAR NOT NULL,
              start_time     TIMESTAMPTZ,
              end_time       TIMESTAMPTZ,
              boards         INT NOT NULL,
              team1_club_id  BIGINT REFERENCES club (club_id),
              team1_name     VARCHAR NOT NULL,
              team1_score    DOUBLE PRECISION NOT NULL,
              team1_result   VARCHAR,
              team2_club_id  BIGINT REFERENCES club (club_id),
              team2_name     VARCHAR NOT NULL,
              team2_score    DOUBLE PRECISION NOT NULL,
              team2_result   VARCHAR,
              fetched_at     TIMESTAMPTZ NOT NULL
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_team1_club ON club_match (team1_club_id)""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_team2_club ON club_match (team2_club_id)""".update.run()
    }

  def selectId(matchId: ClubMatchId): ZIO[Transactor, SQLException, Option[ClubMatch]] =
    connectZIO(repo.findById(matchId))

  def selectMatchIdsForClub(clubId: ClubId): ZIO[Transactor, SQLException, Set[ClubMatchId]] =
    connectZIO {
      sql"""SELECT match_id FROM club_match
            WHERE team1_club_id = $clubId OR team2_club_id = $clubId"""
        .query[ClubMatchId].run().toSet
    }

  def selectStaleForClub(clubId: ClubId): ZIO[Transactor, SQLException, List[ClubMatchId]] =
    connectZIO {
      sql"""SELECT match_id FROM club_match
            WHERE (team1_club_id = $clubId OR team2_club_id = $clubId)
            AND (status != 'Finished' OR fetched_at < end_time + INTERVAL '90 days')"""
        .query[ClubMatchId].run().toList
    }

  def upsert(item: ClubMatch): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_match (match_id, name, url, status, time_class, start_time, end_time, boards,
              team1_club_id, team1_name, team1_score, team1_result,
              team2_club_id, team2_name, team2_score, team2_result, fetched_at)
            VALUES (${item.matchId}, ${item.name}, ${item.url}, ${item.status.toString}, ${item.timeClass.toString},
              ${item.startTime}, ${item.endTime}, ${item.boards},
              ${item.team1ClubId}, ${item.team1Name}, ${item.team1Score}, ${item.team1Result.map(_.toString)},
              ${item.team2ClubId}, ${item.team2Name}, ${item.team2Score}, ${item.team2Result.map(_.toString)},
              ${item.fetchedAt})
            ON CONFLICT (match_id) DO UPDATE SET
              name = EXCLUDED.name, url = EXCLUDED.url, status = EXCLUDED.status,
              time_class = EXCLUDED.time_class, start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time,
              boards = EXCLUDED.boards,
              team1_club_id = EXCLUDED.team1_club_id, team1_name = EXCLUDED.team1_name,
              team1_score = EXCLUDED.team1_score, team1_result = EXCLUDED.team1_result,
              team2_club_id = EXCLUDED.team2_club_id, team2_name = EXCLUDED.team2_name,
              team2_score = EXCLUDED.team2_score, team2_result = EXCLUDED.team2_result,
              fetched_at = EXCLUDED.fetched_at""".update.run()
    }

  def countForClub(clubId: ClubId): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"""SELECT COUNT(*) FROM club_match
            WHERE team1_club_id = $clubId OR team2_club_id = $clubId"""
        .query[Long].run().head
    }
}
