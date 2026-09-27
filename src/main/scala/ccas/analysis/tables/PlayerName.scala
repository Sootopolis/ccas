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

  /** Opens the name `player` stores for every player with no `player_name` row, from the migration instant as
    * [[ClubName.backfill]] does: nothing observed an earlier start (ADR 0016). Idempotent. A name another player is
    * already recorded holding is skipped rather than failing the boot, which leaves this player holding none.
    */
  def backfill: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_name (player_id, username, since)
            SELECT p.player_id, p.username, now() FROM player p
            WHERE NOT EXISTS (SELECT 1 FROM player_name n WHERE n.player_id = p.player_id)
            ON CONFLICT DO NOTHING""".update.run()
    }

  def selectPlayer(playerId: PlayerId): ZIO[PostgresClient, SQLException, List[PlayerName]] =
    connectZIO {
      sql"SELECT $selectCols FROM player_name WHERE player_id = $playerId ORDER BY since".query[PlayerName].run().toList
    }

  /** Every window each of `playerIds` has held a name over, in one round trip. */
  def selectPlayers(playerIds: Iterable[PlayerId]): ZIO[PostgresClient, SQLException, List[PlayerName]] =
    if (playerIds.isEmpty) { ZIO.succeed(Nil) }
    else {
      connectZIO {
        val ids = playerIds.toList
        sql"SELECT $selectCols FROM player_name WHERE player_id = ANY($ids) ORDER BY player_id, since"
          .query[PlayerName].run().toList
      }
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

  /** Brings the current names of the rows a write in the same transaction landed on in line with what `player.username`
    * stores, the way [[ClubName.record]] does for a club: a stored name that does not already stand closes the
    * player's current name and any other player's hold on it, then opens. A row landed on is one storing the `since`
    * and name the write gave it, so a guarded update that lost its race, or an insert that found the row already
    * there, records nothing. That matters for a player holding no name, whose display cache still carries the name
    * another player took: reading it back would take the name from its holder.
    */
  private[tables] def recordStored(written: Iterable[Player])(using DbTx): Int =
    written.groupMap(_.since)(p => p.playerId -> p.username).toList.map { (since, writes) =>
      val writtenNames = writes.toMap
      val ids          = writtenNames.keys.toList
      sql"""SELECT p.player_id, p.username, clock_timestamp() FROM player p
            LEFT JOIN player_name n ON n.player_id = p.player_id AND n.until IS NULL
            WHERE p.player_id = ANY($ids) AND p.since = $since AND n.username IS DISTINCT FROM p.username
            ORDER BY p.player_id""".query[(PlayerId, Username, Instant)].run()
        .filter((playerId, username, _) => writtenNames.get(playerId).contains(username))
        .map((playerId, username, at) => supersede(playerId, username, at)).sum
    }.sum

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
}
