package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, ClubMatchId}
import ccas.utils.sql.SqlZioTypes.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class ClubMatchRef(
  @Id clubId: ClubId,
  matchId: ClubMatchId,
  teamIdx: Int)
    derives DbCodec

object ClubMatchRef {
  private val repo = ImmutableRepo[ClubMatchRef, ClubId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_match_ref (
              club_id  BIGINT PRIMARY KEY REFERENCES club (club_id),
              match_id BIGINT NOT NULL,
              team_idx SMALLINT NOT NULL
            )""".update.run()
    }

  def selectId(clubId: ClubId): ZIO[Transactor, SQLException, Option[ClubMatchRef]] =
    connectZIO(repo.findById(clubId))

  def upsert(ref: ClubMatchRef): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_match_ref (club_id, match_id, team_idx)
            VALUES (${ref.clubId}, ${ref.matchId}, ${ref.teamIdx})
            ON CONFLICT (club_id) DO UPDATE SET match_id = EXCLUDED.match_id, team_idx = EXCLUDED.team_idx""".update.run()
    }

  def deleteId(clubId: ClubId): ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM club_match_ref WHERE club_id = $clubId".update.run())

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM club_match_ref".update.run())
}
