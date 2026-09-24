package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.Title
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class Player(
  @Id playerId: PlayerId,
  joined: Instant,
  username: Username,
  status: PlayerStatusCategory,
  title: Option[Title],
  since: Instant
) derives DbCodec {

  def toSnapshot: PlayerSnapshot =
    PlayerSnapshot(playerId, since, username, status, title)

  def stateMatches(username: Username, status: PlayerStatusCategory, title: Option[Title]): Boolean =
    this.username == username && this.status == status && this.title == title
}

object Player {
  private val repo = Repo[Player, Player, PlayerId]

  /** The tombstone format `PlayerUpdater.archiveAndUpdate` writes, bound as a parameter where SQL has to match it. */
  private[tables] val TombstoneUsernameRegex: String = "^_stale_[0-9]+$"

  private val stalePattern = TombstoneUsernameRegex.r

  /** True when the given username matches the tombstone format set by `PlayerUpdater.archiveAndUpdate`. Useful at
    * display sites that hold a `Username` value but no full `Player` row.
    */
  def isTombstoneUsername(u: Username): Boolean = stalePattern.matches(u.value)

  /** Renders a username for user-facing output, replacing tombstone placeholders with `<unknown player #<id>>`. */
  def displayUsername(username: Username, playerId: PlayerId): String =
    if (isTombstoneUsername(username)) { s"<unknown player #${PlayerId.unwrap(playerId)}>" }
    else { username.value }

  private val selectCols = SqlLiteral("player_id, joined, username, status, title, since")

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player (
              player_id BIGINT PRIMARY KEY,
              joined    TIMESTAMPTZ NOT NULL,
              username  TEXT NOT NULL,
              status    TEXT NOT NULL,
              title     TEXT,
              since     TIMESTAMPTZ NOT NULL,
              CONSTRAINT player_username_unique UNIQUE (username) DEFERRABLE INITIALLY DEFERRED
            )""".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[Player]] =
    connectZIO(repo.findAll.toList)

  def selectId(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[Player]] =
    connectZIO(repo.findById(playerId))

  def selectByIds(playerIds: Iterable[PlayerId]): ZIO[PostgresClient, SQLException, List[Player]] =
    if (playerIds.isEmpty) { ZIO.succeed(Nil) }
    else {
      connectZIO {
        val ids = playerIds.toList
        sql"SELECT $selectCols FROM player WHERE player_id = ANY($ids)".query[Player].run().toList
      }
    }

  /** The display cache for each of `playerIds`, for output that only shows a name. A name to look up, fetch or invite
    * comes from [[PlayerName.selectCurrentNames]] instead (ADR 0016).
    */
  def selectDisplayNames(playerIds: Iterable[PlayerId]): ZIO[PostgresClient, SQLException, Map[PlayerId, Username]] =
    if (playerIds.isEmpty) { ZIO.succeed(Map.empty) }
    else {
      connectZIO {
        val ids = playerIds.toList
        sql"SELECT player_id, username FROM player WHERE player_id = ANY($ids)"
          .query[(PlayerId, Username)].run().map((id, u) => id -> u).toMap
      }
    }

  def selectIdForUpdate(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[Player]] =
    connectZIO(
      sql"SELECT $selectCols FROM player WHERE player_id = $playerId FOR UPDATE".query[Player].run().headOption
    )

  /** The row whose display cache holds `username`, which `player_username_unique` still constrains — the conflict
    * `PlayerUpdater` clears before writing. Not a lookup: a player is found by name through [[PlayerName]].
    */
  def selectByUsernameForUpdate(username: Username): ZIO[PostgresClient, SQLException, Option[Player]] =
    connectZIO(
      sql"SELECT $selectCols FROM player WHERE username = $username FOR UPDATE".query[Player].run().headOption
    )

  // Every write of `player.username` records the name in the same transaction (`PlayerName.recordStored`), so
  // `player_name` never drifts from what the row stores.
  def insert(player: Player): ZIO[PostgresClient, SQLException, Unit] =
    transactZIO {
      repo.insert(player)
      PlayerName.recordStored(List(player.playerId))
    }.unit

  def insertBatch(players: Iterable[Player]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      val result = batchUpdate(players) { player =>
        sql"""INSERT INTO player (player_id, joined, username, status, title, since)
              VALUES (${player.playerId}, ${player.joined}, ${player.username},
                ${player.status}, ${player.title}, ${player.since})
              ON CONFLICT (player_id) DO NOTHING""".update
      }
      PlayerName.recordStored(players.map(_.playerId))
      result
    }

  def insertIfNew(player: Player): ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      val rows =
        sql"""INSERT INTO player (player_id, joined, username, status, title, since)
              VALUES (${player.playerId}, ${player.joined}, ${player.username},
                ${player.status}, ${player.title}, ${player.since})
              ON CONFLICT (player_id) DO NOTHING""".update.run()
      PlayerName.recordStored(List(player.playerId))
      rows
    }

  // Optimistic update: `AND since < newSince` makes concurrent updates monotonic. If another
  // writer has already advanced `since` past ours, our UPDATE no-ops instead of overwriting their
  // fresher data. Protects against lost updates when two MembershipApp runs for different clubs
  // both classify a shared player from the same stale state.
  def updateCurrentState(player: Player): ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      val rows =
        sql"""UPDATE player SET username = ${player.username}, status = ${player.status},
                title = ${player.title}, since = ${player.since}
              WHERE player_id = ${player.playerId} AND since < ${player.since}""".update.run()
      PlayerName.recordStored(List(player.playerId))
      rows
    }

  def updateCurrentStateBatch(players: Iterable[Player]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      val result = batchUpdate(players) { player =>
        sql"""UPDATE player SET username = ${player.username}, status = ${player.status},
                title = ${player.title}, since = ${player.since}
              WHERE player_id = ${player.playerId} AND since < ${player.since}""".update
      }
      PlayerName.recordStored(players.map(_.playerId))
      result
    }
}
