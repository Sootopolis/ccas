package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO
import RefSkipReason.given

import ccas.api.misc.subtypes.PlayerId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class PlayerRefSkip(
  playerId: PlayerId,
  reason: RefSkipReason,
  detail: Option[String],
  lastAttempted: Instant
) derives DbCodec

object PlayerRefSkip {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_ref_skip (
              player_id      BIGINT PRIMARY KEY REFERENCES player (player_id) ON DELETE RESTRICT,
              reason         TEXT NOT NULL,
              detail         TEXT,
              last_attempted TIMESTAMPTZ NOT NULL
            )""".update.run()
    }

  def selectId(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[PlayerRefSkip]] =
    connectZIO {
      sql"SELECT player_id, reason, detail, last_attempted FROM player_ref_skip WHERE player_id = $playerId"
        .query[PlayerRefSkip].run().headOption
    }

  def countByReason: ZIO[PostgresClient, SQLException, List[SkipCount]] =
    connectZIO {
      sql"SELECT reason, COUNT(*) FROM player_ref_skip GROUP BY reason"
        .query[(RefSkipReason, Long)].run().map((r, c) => SkipCount(r, c)).toList
    }

  def upsert(item: PlayerRefSkip): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_ref_skip (player_id, reason, detail, last_attempted)
            VALUES (${item.playerId}, ${item.reason}, ${item.detail}, ${item.lastAttempted})
            ON CONFLICT (player_id) DO UPDATE SET
              reason = EXCLUDED.reason,
              detail = EXCLUDED.detail,
              last_attempted = EXCLUDED.last_attempted""".update.run()
    }

  def deleteId(playerId: PlayerId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO(sql"DELETE FROM player_ref_skip WHERE player_id = $playerId".update.run())
}
