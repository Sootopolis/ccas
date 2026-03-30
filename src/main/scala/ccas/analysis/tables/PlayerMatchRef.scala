package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubMatchId, PlayerId}
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class PlayerMatchRef(
  @Id playerId: PlayerId,
  matchId: ClubMatchId,
  isLive: Boolean,
  isTeam1: Boolean,
  boardIdx: Short
) derives DbCodec

object PlayerMatchRef {
  private val repo = ImmutableRepo[PlayerMatchRef, PlayerId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_match_ref (
              player_id  BIGINT PRIMARY KEY REFERENCES player (player_id) ON DELETE RESTRICT,
              match_id   BIGINT NOT NULL,
              is_live    BOOLEAN NOT NULL,
              is_team1   BOOLEAN NOT NULL,
              board_idx  SMALLINT NOT NULL
            )""".update.run()
    }

  def selectId(playerId: PlayerId): ZIO[Transactor, SQLException, Option[PlayerMatchRef]] =
    connectZIO(repo.findById(playerId))

  def insert(ref: PlayerMatchRef): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_match_ref (player_id, match_id, is_live, is_team1, board_idx)
            VALUES (${ref.playerId}, ${ref.matchId}, ${ref.isLive}, ${ref.isTeam1}, ${ref.boardIdx})""".update.run()
    }

  def insertBatch(refs: Iterable[PlayerMatchRef]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(refs) { ref =>
        sql"""INSERT INTO player_match_ref (player_id, match_id, is_live, is_team1, board_idx)
              VALUES (${ref.playerId}, ${ref.matchId}, ${ref.isLive}, ${ref.isTeam1}, ${ref.boardIdx})""".update
      }
    }

  def deleteId(playerId: PlayerId): ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM player_match_ref WHERE player_id = $playerId".update.run())

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM player_match_ref".update.run())
}
