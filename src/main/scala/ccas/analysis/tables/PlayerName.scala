package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

/** One window over which a player was observed holding a username — [[ClubName]]'s shape for players, bounds and
  * constraints included (ADR 0016). `player.username` is the display cache; a username that is looked up, fetched or
  * handed to someone to act on comes from here.
  */
final case class PlayerName(playerId: PlayerId, username: Username, since: Instant, until: Option[Instant])
    derives DbCodec

object PlayerName {
  private val selectCols = SqlLiteral("player_id, username, since, until")

  /** Fails with a pointer to the runbook when `btree_gist` is missing: see [[BtreeGist]]. */
  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      BtreeGist.require("player_name")
      sql"""CREATE TABLE IF NOT EXISTS player_name (
              player_id  BIGINT      NOT NULL REFERENCES player (player_id) ON DELETE RESTRICT,
              username   TEXT        NOT NULL,
              since      TIMESTAMPTZ NOT NULL,
              until      TIMESTAMPTZ,
              PRIMARY KEY (player_id, since),
              CONSTRAINT player_name_window CHECK (until IS NULL OR until > since),
              CONSTRAINT player_name_no_overlap
                EXCLUDE USING gist (player_id WITH =, tstzrange(since, until) WITH &&)
            )""".update.run()
      sql"CREATE UNIQUE INDEX IF NOT EXISTS player_name_current ON player_name (username) WHERE until IS NULL"
        .update.run()
    }

  /** Seeds every player with no `player_name` row from the history `player_snapshot` and `player` hold: a window per
    * run of one username, closed where the next state begins and left open for the name held now. A tombstone holds
    * nothing, so it only closes the name before it. A snapshot dated at or after the row's own `since` is ignored, so
    * the open name is always the one `player` stores. Idempotent. A name that another player is already recorded
    * holding is skipped rather than failing the boot, which leaves this player holding none.
    */
  def backfill: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_name (player_id, username, since, until)
            WITH fresh AS (
              SELECT player_id, username, since FROM player p
              WHERE NOT EXISTS (SELECT 1 FROM player_name n WHERE n.player_id = p.player_id)
            ),
            observed AS (
              SELECT s.player_id, s.username, s.since
              FROM player_snapshot s JOIN fresh f ON f.player_id = s.player_id
              WHERE s.since < f.since
              UNION ALL
              SELECT player_id, username, since FROM fresh
            ),
            marked AS (
              SELECT player_id, username, since,
                     lag(username) OVER (PARTITION BY player_id ORDER BY since) AS previous
              FROM observed
            ),
            windows AS (
              SELECT player_id, username, since,
                     lead(since) OVER (PARTITION BY player_id ORDER BY since) AS until
              FROM marked
              WHERE previous IS DISTINCT FROM username
            )
            SELECT player_id, username, since, until FROM windows
            WHERE username !~ ${Player.TombstoneUsernameRegex}
            ON CONFLICT DO NOTHING""".update.run()
    }

  def selectPlayer(playerId: PlayerId): ZIO[PostgresClient, SQLException, List[PlayerName]] =
    connectZIO {
      sql"SELECT $selectCols FROM player_name WHERE player_id = $playerId ORDER BY since".query[PlayerName].run().toList
    }

  /** The player that holds `username` now, if one does. */
  def selectCurrentHolder(username: Username): ZIO[PostgresClient, SQLException, Option[PlayerId]] =
    connectZIO {
      sql"SELECT player_id FROM player_name WHERE username = $username AND until IS NULL".query[PlayerId].run()
        .headOption
    }

  /** The holder of each of `usernames` that some player holds now, in one round trip. */
  def selectCurrentHolders(usernames: Iterable[Username]): ZIO[PostgresClient, SQLException, Map[Username, PlayerId]] =
    if (usernames.isEmpty) { ZIO.succeed(Map.empty) }
    else {
      connectZIO {
        val names = usernames.toList
        sql"SELECT username, player_id FROM player_name WHERE until IS NULL AND username = ANY($names)"
          .query[(Username, PlayerId)].run().toMap
      }
    }

  /** Every player that has ever held `username`, current holder included. Unindexed on purpose — ask
    * [[selectCurrentHolder]] first and come here only on a miss (ADR 0016).
    */
  def selectHolders(username: Username): ZIO[PostgresClient, SQLException, List[PlayerId]] =
    connectZIO {
      sql"SELECT DISTINCT player_id FROM player_name WHERE username = $username ORDER BY player_id"
        .query[PlayerId].run().toList
    }

  /** The username `playerId` holds now, if it holds one: none once another player has taken it and we have not yet seen
    * the name it answers to.
    */
  def selectCurrentName(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[Username]] =
    connectZIO {
      sql"SELECT username FROM player_name WHERE player_id = $playerId AND until IS NULL".query[Username].run()
        .headOption
    }

  /** The username each of `playerIds` holds now, in one round trip. A player absent from the answer holds none. */
  def selectCurrentNames(playerIds: Iterable[PlayerId]): ZIO[PostgresClient, SQLException, Map[PlayerId, Username]] =
    if (playerIds.isEmpty) { ZIO.succeed(Map.empty) }
    else {
      connectZIO {
        val ids = playerIds.toList
        sql"SELECT player_id, username FROM player_name WHERE until IS NULL AND player_id = ANY($ids)"
          .query[(PlayerId, Username)].run().toMap
      }
    }

  /** Brings the current names of `playerIds` in line with what `player.username` stores after a write in the same
    * transaction, the way [[ClubName.record]] does for a club: a stored name that does not already stand closes the
    * player's current name and any other player's hold on it, then opens. A tombstone opens nothing, so its player is
    * left holding none. Reading what is stored rather than what was written is what makes a guarded update that lost
    * its race record nothing: it changed no row, and one statement compares a consistent snapshot of both tables.
    */
  private[tables] def recordStored(playerIds: Iterable[PlayerId])(using DbTx): Int = {
    val ids = playerIds.toList.distinct
    if (ids.isEmpty) { 0 }
    else {
      val drifted =
        sql"""SELECT p.player_id, p.username, clock_timestamp() FROM player p
              LEFT JOIN player_name n ON n.player_id = p.player_id AND n.until IS NULL
              WHERE p.player_id = ANY($ids) AND n.username IS DISTINCT FROM p.username
              ORDER BY p.player_id""".query[(PlayerId, Username, Instant)].run()
      drifted.map { (playerId, username, at) =>
        if (Player.isTombstoneUsername(username)) { close(playerId, at) }
        else { supersede(playerId, username, at) }
      }.sum
    }
  }

  // A row opened at or after `at` would close into an empty window, which the CHECK rejects and which records nothing,
  // so it is dropped instead of closed.
  private def supersede(playerId: PlayerId, username: Username, at: Instant)(using DbTx): Int = {
    val dropped =
      sql"""DELETE FROM player_name
            WHERE until IS NULL AND since >= $at
              AND ((player_id = $playerId AND username <> $username)
                OR (username = $username AND player_id <> $playerId))""".update.run()
    val closed =
      sql"""UPDATE player_name SET until = $at
            WHERE until IS NULL
              AND ((player_id = $playerId AND username <> $username)
                OR (username = $username AND player_id <> $playerId))""".update.run()
    val opened =
      sql"INSERT INTO player_name (player_id, username, since) VALUES ($playerId, $username, $at)".update.run()
    dropped + closed + opened
  }

  private def close(playerId: PlayerId, at: Instant)(using DbTx): Int = {
    val dropped =
      sql"DELETE FROM player_name WHERE player_id = $playerId AND until IS NULL AND since >= $at".update.run()
    val closed = sql"UPDATE player_name SET until = $at WHERE player_id = $playerId AND until IS NULL".update.run()
    dropped + closed
  }
}
