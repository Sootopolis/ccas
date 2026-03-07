package ccas.analysis.tables

import ccas.api.misc.subtypes.PlayerId
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.SqlRepoUtils
import io.getquill.*
import io.getquill.jdbczio.Quill

import java.time.Instant

case class Player(playerId: PlayerId, joined: Instant)

object Player extends SqlRepoUtils {
  override protected type Repo = PlayerRepository

  override protected def makeRepo(quill: Quill.Postgres[SnakeCase]): Repo = PlayerRepository(quill)

  def selectAll: RepoTask[List[Player]] = repoService(_.selectAll)
  def selectId(playerId: PlayerId): RepoTask[Option[Player]] = repoService(_.selectId(playerId))
  def insert(player: Player): RepoTask[Unit] = repoService(_.insert(player))
  def insertBatch(players: Iterable[Player]): RepoTask[Unit] = repoService(_.insertBatch(players))
  def deleteAll: RepoTask[Unit] = repoService(_.deleteAll)
  def deleteId(playerId: PlayerId): RepoTask[Unit] = repoService(_.deleteId(playerId))

  case class PlayerRepository(quill: Quill.Postgres[SnakeCase]) {
    import quill.*

    inline def selectAllQuery = query[Player]
    inline def selectIdQuery(playerId: PlayerId) = selectAllQuery.filter(_.playerId == lift(playerId))
    private inline def insertLifted(player: Player): Insert[Player] = selectAllQuery.insertValue(player)
    inline def insertQuery(player: Player) = insertLifted(lift(player))
    inline def insertBatchQuery(players: Iterable[Player]) = liftQuery(players).foreach(insertLifted)
    inline def deleteAllQuery = selectAllQuery.delete
    inline def deleteQuery(playerId: PlayerId) = selectIdQuery(playerId).delete

    def selectAll: SqlTask[List[Player]] = run(selectAllQuery)
    def selectId(playerId: PlayerId): SqlTask[Option[Player]] = run(selectIdQuery(playerId)).map(_.headOption)
    def insert(player: Player): SqlTask[Unit] = run(insertQuery(player)).unit
    def insertBatch(players: Iterable[Player]): SqlTask[Unit] = run(insertBatchQuery(players)).unit
    def deleteAll: SqlTask[Unit] = run(deleteAllQuery).unit
    def deleteId(playerId: PlayerId): SqlTask[Unit] = run(deleteQuery(playerId)).unit
  }
}
