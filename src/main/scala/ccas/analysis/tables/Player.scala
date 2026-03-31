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

  private val selectCols = SqlLiteral("player_id, joined, username, status, title, since")

  def createTable: ZIO[Transactor, SQLException, Int] =
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

  def selectAll: ZIO[Transactor, SQLException, List[Player]] =
    connectZIO(repo.findAll.toList)

  def selectId(playerId: PlayerId): ZIO[Transactor, SQLException, Option[Player]] =
    connectZIO(repo.findById(playerId))

  def selectByIds(playerIds: Iterable[PlayerId]): ZIO[Transactor, SQLException, List[Player]] =
    if (playerIds.isEmpty) { ZIO.succeed(Nil) }
    else {
      connectZIO {
        val ids = playerIds.map(id => PlayerId.unwrap(id).toString).mkString(",")
        sql"SELECT $selectCols FROM player WHERE player_id IN (${SqlLiteral(ids)})".query[Player].run().toList
      }
    }

  def resolveUsernames(playerIds: Iterable[PlayerId]): ZIO[Transactor, SQLException, Map[PlayerId, Username]] =
    if (playerIds.isEmpty) { ZIO.succeed(Map.empty) }
    else {
      connectZIO {
        val ids = playerIds.map(id => PlayerId.unwrap(id).toString).mkString(",")
        sql"SELECT player_id, username FROM player WHERE player_id IN (${SqlLiteral(ids)})"
          .query[(PlayerId, Username)].run().map((id, u) => id -> u).toMap
      }
    }

  def selectByUsername(username: Username): ZIO[Transactor, SQLException, Option[Player]] =
    connectZIO(
      sql"SELECT $selectCols FROM player WHERE username = $username".query[Player].run().headOption
    )

  def selectIdForUpdate(playerId: PlayerId): ZIO[Transactor, SQLException, Option[Player]] =
    connectZIO(
      sql"SELECT $selectCols FROM player WHERE player_id = $playerId FOR UPDATE".query[Player].run().headOption
    )

  def selectByUsernameForUpdate(username: Username): ZIO[Transactor, SQLException, Option[Player]] =
    connectZIO(
      sql"SELECT $selectCols FROM player WHERE username = $username FOR UPDATE".query[Player].run().headOption
    )

  def insert(player: Player): ZIO[Transactor, SQLException, Unit] =
    connectZIO(repo.insert(player))

  def insertBatch(players: Iterable[Player]): ZIO[Transactor, SQLException, Unit] =
    transactZIO(repo.insertAll(players))

  def insertIfNew(player: Player): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player (player_id, joined, username, status, title, since)
            VALUES (${player.playerId}, ${player.joined}, ${player.username},
              ${player.status.toString}, ${player.title.map(_.toString)}, ${player.since})
            ON CONFLICT (player_id) DO NOTHING""".update.run()
    }

  def updateCurrentState(player: Player): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE player SET username = ${player.username}, status = ${player.status.toString},
              title = ${player.title.map(_.toString)}, since = ${player.since}
            WHERE player_id = ${player.playerId}""".update.run()
    }

  def updateCurrentStateBatch(players: Iterable[Player]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(players) { player =>
        sql"""UPDATE player SET username = ${player.username}, status = ${player.status.toString},
                title = ${player.title.map(_.toString)}, since = ${player.since}
              WHERE player_id = ${player.playerId}""".update
      }
    }

  def deleteId(playerId: PlayerId): ZIO[Transactor, SQLException, Unit] =
    connectZIO(repo.deleteById(playerId))

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM player".update.run())
}
