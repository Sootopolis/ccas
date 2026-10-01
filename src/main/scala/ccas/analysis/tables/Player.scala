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
    PlayerSnapshot(playerId, since, status, title)

  /** Whether an observation matches this row, given `heldNameOption`: the name the player holds now, from
    * [[PlayerName]]. Never `username`, which a player holding none keeps as a display label — a re-observation of that
    * very name would match it and record nothing, leaving the player holding none (ADR 0016).
    */
  def stateMatches(
    heldNameOption: Option[Username],
    username: Username,
    status: PlayerStatusCategory,
    title: Option[Title]
  ): Boolean =
    heldNameOption.contains(username) && this.status == status && this.title == title
}

object Player {
  private val repo = Repo[Player, Player, PlayerId]

  private val selectCols = SqlLiteral("player_id, joined, username, status, title, since")

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player (
              player_id BIGINT PRIMARY KEY,
              joined    TIMESTAMPTZ NOT NULL,
              username  TEXT NOT NULL,
              status    TEXT NOT NULL,
              title     TEXT,
              since     TIMESTAMPTZ NOT NULL
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

  /** What output calls each of `playerIds`, through [[displayName]]. A name to look up, fetch or invite comes from
    * [[PlayerName.selectCurrentNames]] instead (ADR 0016).
    */
  def selectDisplayNames(playerIds: Iterable[PlayerId]): ZIO[PostgresClient, SQLException, Map[PlayerId, String]] =
    if (playerIds.isEmpty) { ZIO.succeed(Map.empty) }
    else {
      connectZIO {
        val ids = playerIds.toList
        sql"""SELECT p.player_id, p.username, n.player_id IS NOT NULL FROM player p
              LEFT JOIN player_name n ON n.player_id = p.player_id AND n.until IS NULL
              WHERE p.player_id = ANY($ids)"""
          .query[(PlayerId, Username, Boolean)].run().map((id, u, holds) => id -> displayName(id, u, holds)).toMap
      }
    }

  /** A player as output shows it: the name it holds, or — once another player has taken that name and we have not seen
    * the one it moved to — the last it held, marked so the two never read as one player.
    */
  private def displayName(playerId: PlayerId, username: Username, holdsName: Boolean): String =
    if (holdsName) { username.value }
    else { s"<unknown player #${PlayerId.unwrap(playerId)}, was ${username.value}>" }

  def selectIdForUpdate(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[Player]] =
    connectZIO(
      sql"SELECT $selectCols FROM player WHERE player_id = $playerId FOR UPDATE".query[Player].run().headOption
    )

  // Every write of `player.username` records the name in the same transaction (`PlayerName.recordStored`): a row the
  // write lands on holds the name it stores.
  def insert(player: Player): ZIO[PostgresClient, SQLException, Unit] =
    transactZIO {
      repo.insert(player)
      PlayerName.recordStored(List(player))
    }.unit

  def insertIfNew(player: Player): ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      val rows = insertIfNewFrag(player).run()
      PlayerName.recordStored(List(player))
      rows
    }

  def updateCurrentState(player: Player): ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      val rows = updateCurrentStateFrag(player).run()
      PlayerName.recordStored(List(player))
      rows
    }

  /** [[insertIfNew]] for each of `inserted` and [[updateCurrentState]] for each of `updated`, recording the names both
    * store in one call, since a transaction must record names only once (ADR 0020).
    */
  def writeBatch(
    inserted: Iterable[Player],
    updated: Iterable[Player]
  ): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      val insertedRows = batchUpdate(inserted)(insertIfNewFrag)
      val updatedRows  = batchUpdate(updated)(updateCurrentStateFrag)
      PlayerName.recordStored(inserted ++ updated)
      (insertedRows, updatedRows) match {
        case (BatchUpdateResult.Success(i), BatchUpdateResult.Success(u)) => BatchUpdateResult.Success(i + u)
        case _                                                            => BatchUpdateResult.SuccessNoInfo
      }
    }

  private def insertIfNewFrag(player: Player): Update =
    sql"""INSERT INTO player (player_id, joined, username, status, title, since)
          VALUES (${player.playerId}, ${player.joined}, ${player.username},
            ${player.status}, ${player.title}, ${player.since})
          ON CONFLICT (player_id) DO NOTHING""".update

  // Optimistic update: `AND since < newSince` makes concurrent updates monotonic. If another writer has already
  // advanced `since` past ours, our UPDATE no-ops instead of overwriting their fresher data. Protects against lost
  // updates when two MembershipApp runs for different clubs both classify a shared player from the same stale state.
  private def updateCurrentStateFrag(player: Player): Update =
    sql"""UPDATE player SET username = ${player.username}, status = ${player.status},
            title = ${player.title}, since = ${player.since}
          WHERE player_id = ${player.playerId} AND since < ${player.since}""".update
}
