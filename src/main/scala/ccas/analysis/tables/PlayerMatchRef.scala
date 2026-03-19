package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubMatchId, PlayerId}
import ccas.utils.sql.SqlZioTypes.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class PlayerMatchRef(@Id playerId: PlayerId, matchId: ClubMatchId, teamIdx: Int, boardIdx: Int)
    derives DbCodec

object PlayerMatchRef {
  private val repo = ImmutableRepo[PlayerMatchRef, PlayerId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_match_ref (
              player_id BIGINT PRIMARY KEY REFERENCES player (player_id),
              match_id  BIGINT NOT NULL,
              team_idx  SMALLINT NOT NULL,
              board_idx SMALLINT NOT NULL
            )""".update.run()
    }

  def selectId(playerId: PlayerId): ZIO[Transactor, SQLException, Option[PlayerMatchRef]] =
    connectZIO(repo.findById(playerId))

  def upsert(ref: PlayerMatchRef): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_match_ref (player_id, match_id, team_idx, board_idx)
            VALUES (${ref.playerId}, ${ref.matchId}, ${ref.teamIdx}, ${ref.boardIdx})
            ON CONFLICT (player_id) DO UPDATE SET match_id = EXCLUDED.match_id, team_idx = EXCLUDED.team_idx, board_idx = EXCLUDED.board_idx""".update.run()
    }

  def deleteId(playerId: PlayerId): ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM player_match_ref WHERE player_id = $playerId".update.run())

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM player_match_ref".update.run())
}
