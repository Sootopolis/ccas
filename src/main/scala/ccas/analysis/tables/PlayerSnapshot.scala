package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.Title
import ccas.api.misc.subtypes.PlayerId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

/** A player's status and title from `since` until the next snapshot or the `player` row. Its names are
  * [[PlayerName]]'s (ADR 0016).
  */
final case class PlayerSnapshot(playerId: PlayerId, since: Instant, status: PlayerStatusCategory, title: Option[Title])
    derives DbCodec

object PlayerSnapshot {
  private val selectCols = SqlLiteral("player_id, since, status, title")

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_snapshot (
              player_id BIGINT NOT NULL,
              since     TIMESTAMPTZ NOT NULL,
              status    TEXT NOT NULL,
              title     TEXT,
              PRIMARY KEY (player_id, since),
              FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT
            )""".update.run()
    }

  /** All historical snapshots for a player. */
  def selectId(playerId: PlayerId): ZIO[PostgresClient, SQLException, List[PlayerSnapshot]] =
    connectZIO(
      sql"SELECT $selectCols FROM player_snapshot WHERE player_id = $playerId".query[PlayerSnapshot].run().toList
    )

  /** Every state each of `playerIds` has been in, unordered: the history `player_snapshot` holds and the current state
    * `player` holds.
    */
  def selectHistory(playerIds: Iterable[PlayerId]): ZIO[PostgresClient, SQLException, List[PlayerSnapshot]] =
    if (playerIds.isEmpty) { ZIO.succeed(Nil) }
    else {
      connectZIO {
        val ids = playerIds.toList
        sql"""SELECT $selectCols FROM player_snapshot WHERE player_id = ANY($ids)
              UNION ALL
              SELECT $selectCols FROM player WHERE player_id = ANY($ids)""".query[PlayerSnapshot].run().toList
      }
    }

  def insert(item: PlayerSnapshot): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_snapshot (player_id, since, status, title)
            VALUES (${item.playerId}, ${item.since}, ${item.status}, ${item.title})
            ON CONFLICT (player_id, since) DO NOTHING""".update.run()
    }

  def insertBatch(items: Iterable[PlayerSnapshot]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO player_snapshot (player_id, since, status, title)
              VALUES (${item.playerId}, ${item.since}, ${item.status}, ${item.title})
              ON CONFLICT (player_id, since) DO NOTHING""".update
      }
    }

  def update(item: PlayerSnapshot): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE player_snapshot SET status = ${item.status}, title = ${item.title}
            WHERE player_id = ${item.playerId} AND since = ${item.since}""".update.run()
    }
}
