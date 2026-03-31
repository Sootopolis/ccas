package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.Title
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

final case class PlayerSnapshot(
  playerId: PlayerId,
  since: Instant,
  username: Username,
  status: PlayerStatusCategory,
  title: Option[Title]
) derives DbCodec

object PlayerSnapshot {
  private val selectCols = SqlLiteral("player_id, since, username, status, title")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_snapshot (
              player_id BIGINT NOT NULL,
              since     TIMESTAMPTZ NOT NULL,
              username  TEXT NOT NULL,
              status    TEXT NOT NULL,
              title     TEXT,
              PRIMARY KEY (player_id, since),
              FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_player_snapshot_username
            ON player_snapshot (username, since DESC)""".update.run()
    }

  /** All historical snapshots for a player. */
  def selectId(playerId: PlayerId): ZIO[Transactor, SQLException, List[PlayerSnapshot]] =
    connectZIO(
      sql"SELECT $selectCols FROM player_snapshot WHERE player_id = $playerId".query[PlayerSnapshot].run().toList
    )

  /** All historical snapshots for a username. */
  def selectName(username: Username): ZIO[Transactor, SQLException, List[PlayerSnapshot]] =
    connectZIO(
      sql"SELECT $selectCols FROM player_snapshot WHERE username = $username".query[PlayerSnapshot].run().toList
    )

  /** Historical snapshots plus current player state, for time-range reporting.
    * Returns all snapshots whose `since` is after the given instant, plus
    * the current state from `player` if its `since` falls after the cutoff.
    */
  def selectSince(since: Instant): ZIO[Transactor, SQLException, List[PlayerSnapshot]] =
    connectZIO(
      sql"""WITH all_states AS (
              SELECT $selectCols FROM player_snapshot
              UNION ALL
              SELECT player_id, since, username, status, title FROM player
            )
            (SELECT DISTINCT ON (player_id) $selectCols FROM all_states
             WHERE since <= $since ORDER BY player_id, since DESC)
            UNION ALL
            SELECT $selectCols FROM all_states WHERE since > $since"""
        .query[PlayerSnapshot].run().toList
    )

  def insert(item: PlayerSnapshot): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_snapshot (player_id, since, username, status, title)
            VALUES (${item.playerId}, ${item.since}, ${item.username}, ${item.status.toString}, ${item.title.map(
          _.toString
        )})""".update.run()
    }

  def insertBatch(items: Iterable[PlayerSnapshot]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO player_snapshot (player_id, since, username, status, title)
              VALUES (${item.playerId}, ${item.since}, ${item.username}, ${item.status.toString}, ${item.title.map(
            _.toString
          )})""".update
      }
    }

  def update(item: PlayerSnapshot): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE player_snapshot SET username = ${item.username}, status = ${item.status.toString}, title = ${item
          .title.map(_.toString)}
            WHERE player_id = ${item.playerId} AND since = ${item.since}""".update.run()
    }

  def updateBatch(items: Iterable[PlayerSnapshot]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""UPDATE player_snapshot SET username = ${item.username}, status = ${item.status.toString}, title = ${item
            .title.map(_.toString)}
              WHERE player_id = ${item.playerId} AND since = ${item.since}""".update
      }
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM player_snapshot".update.run()
    }
}
