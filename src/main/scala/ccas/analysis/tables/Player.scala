package ccas.analysis.tables

import ccas.api.misc.subtypes.PlayerId
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.SqlRepoUtils
import ccas.utils.sql.DbCodecs.given
import com.augustnagro.magnum.*
import zio.ZIO

import java.sql.SQLException
import java.time.Instant

case class Player(playerId: PlayerId, joined: Instant) derives DbCodec

object Player extends SqlRepoUtils {
  override protected type Repo = PlayerRepository

  override protected def makeRepo(xa: Transactor): Repo = PlayerRepository(xa)

  // Raw SQL — composable in transact() blocks (writes only)
  def insertSql(player: Player)(using DbCon): Unit = {
    sql"INSERT INTO player (player_id, joined) VALUES (${player.playerId}, ${player.joined})".update.run()
    ()
  }

  def insertBatchSql(players: Iterable[Player])(using DbCon): Unit = {
    batchUpdate(players) { player =>
      sql"INSERT INTO player (player_id, joined) VALUES (${player.playerId}, ${player.joined})".update
    }
    ()
  }

  def deleteAllSql(using DbCon): Unit = {
    sql"DELETE FROM player".update.run()
    ()
  }

  def deleteIdSql(playerId: PlayerId)(using DbCon): Unit = {
    sql"DELETE FROM player WHERE player_id = $playerId".update.run()
    ()
  }

  // ZIO API
  def selectAll: RepoTask[List[Player]] = repoService(_.selectAll)
  def selectId(playerId: PlayerId): RepoTask[Option[Player]] = repoService(_.selectId(playerId))
  def insert(player: Player): RepoTask[Unit] = repoService(_.insert(player))
  def insertBatch(players: Iterable[Player]): RepoTask[Unit] = repoService(_.insertBatch(players))
  def deleteAll: RepoTask[Unit] = repoService(_.deleteAll)
  def deleteId(playerId: PlayerId): RepoTask[Unit] = repoService(_.deleteId(playerId))

  case class PlayerRepository(xa: Transactor) {
    def selectAll: SqlTask[List[Player]] =
      ZIO.attempt { connect(xa)(sql"SELECT player_id, joined FROM player".query[Player].run().toList) }
        .refineToOrDie[SQLException]

    def selectId(playerId: PlayerId): SqlTask[Option[Player]] =
      ZIO.attempt { connect(xa)(sql"SELECT player_id, joined FROM player WHERE player_id = $playerId".query[Player].run().headOption) }
        .refineToOrDie[SQLException]

    def insert(player: Player): SqlTask[Unit] =
      ZIO.attempt { connect(xa)(insertSql(player)) }
        .refineToOrDie[SQLException]

    def insertBatch(players: Iterable[Player]): SqlTask[Unit] =
      ZIO.attempt { transact(xa)(insertBatchSql(players)) }
        .refineToOrDie[SQLException]

    def deleteAll: SqlTask[Unit] =
      ZIO.attempt { connect(xa)(deleteAllSql) }
        .refineToOrDie[SQLException]

    def deleteId(playerId: PlayerId): SqlTask[Unit] =
      ZIO.attempt { connect(xa)(deleteIdSql(playerId)) }
        .refineToOrDie[SQLException]
  }
}
