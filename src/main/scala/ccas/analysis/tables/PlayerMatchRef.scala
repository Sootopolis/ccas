package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubMatchId, PlayerId}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

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

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_match_ref (
              player_id  BIGINT PRIMARY KEY REFERENCES player (player_id) ON DELETE RESTRICT,
              match_id   BIGINT NOT NULL,
              is_live    BOOLEAN NOT NULL,
              is_team1   BOOLEAN NOT NULL,
              board_idx  SMALLINT NOT NULL
            )""".update.run()
    }

  def selectId(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[PlayerMatchRef]] =
    connectZIO(repo.findById(playerId))

  /** Explicit ref if one exists, otherwise an inferred ref (from `club_match_board`) promoted to `player_match_ref` so
    * subsequent callers skip the synthesis tier. Returns `None` only when neither table carries the player.
    */
  def findOrInfer(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[PlayerMatchRef]] =
    selectId(playerId).flatMap {
      case some @ Some(_) => ZIO.succeed(some)
      case None           => ClubMatchBoard.inferPlayerMatchRef(playerId).tap(ZIO.foreachDiscard(_)(upsert))
    }

  def insert(ref: PlayerMatchRef): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_match_ref (player_id, match_id, is_live, is_team1, board_idx)
            VALUES (${ref.playerId}, ${ref.matchId}, ${ref.isLive}, ${ref.isTeam1}, ${ref.boardIdx})""".update.run()
    }

  def upsert(ref: PlayerMatchRef): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_match_ref (player_id, match_id, is_live, is_team1, board_idx)
            VALUES (${ref.playerId}, ${ref.matchId}, ${ref.isLive}, ${ref.isTeam1}, ${ref.boardIdx})
            ON CONFLICT (player_id) DO UPDATE SET
              match_id  = EXCLUDED.match_id,
              is_live   = EXCLUDED.is_live,
              is_team1  = EXCLUDED.is_team1,
              board_idx = EXCLUDED.board_idx""".update.run()
    }

  def insertBatch(refs: Iterable[PlayerMatchRef]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(refs) { ref =>
        sql"""INSERT INTO player_match_ref (player_id, match_id, is_live, is_team1, board_idx)
              VALUES (${ref.playerId}, ${ref.matchId}, ${ref.isLive}, ${ref.isTeam1}, ${ref.boardIdx})""".update
      }
    }

  def deleteId(playerId: PlayerId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO(sql"DELETE FROM player_match_ref WHERE player_id = $playerId".update.run())
}
