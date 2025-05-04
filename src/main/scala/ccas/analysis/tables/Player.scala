package ccas.analysis.tables

import ccas.api.misc.subtypes.PlayerId
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.{RepoResolver, SqlRepoUtils}
import io.getquill.*
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.jdbczio.Quill
import zio.ZIO

import java.time.Instant

case class Player(playerId: PlayerId, joined: Instant)

object Player extends SqlRepoUtils {
  /** Type of the repository of the table, typically a `sealed trait`. */
  override protected type Repo = PlayerRepository

  override protected val repoResolver: RepoResolver[Repo] = RepoResolver(
    postgres = PostgresRepo.apply,
    sqlite = SqliteRepo.apply,
  )

  /** Select all player records. */
  def selectAll: RepoTask[List[Player]] = repoService(_.selectAll)
  /** Select the entry of a player id if exists. */
  def selectId(playerId: PlayerId): RepoTask[Option[Player]] = repoService(_.selectId(playerId))
  /** Insert a player record. */
  def insert(player: Player): RepoTask[Unit] = repoService(_.insert(player))
  /** Insert a collection of player records. */
  def insertBatch(players: Iterable[Player]): RepoTask[Unit] = repoService(_.insertBatch(players))
  /** Delete all player records. Use with caution. */
  def deleteAll: RepoTask[Unit] = repoService(_.deleteAll)
  /** Delete the entry of a player id if exists. */
  def deleteId(playerId: PlayerId): RepoTask[Unit] = repoService(_.deleteId(playerId))

  sealed trait PlayerRepository {
    val quill: Quill[? <: SqlIdiom, SnakeCase]
    import quill.*

    protected inline def selectAllQuery: EntityQuery[Player] = query[Player]
    protected inline def selectIdQuery(playerId: PlayerId): EntityQuery[Player] =
      selectAllQuery.filter(_.playerId == lift(playerId))
    protected inline def insertQuery(player: Player): Insert[Player] = selectAllQuery.insertValue(lift(player))
    protected inline def insertBatchQuery(players: Iterable[Player]): BatchAction[Insert[Player]] =
      liftQuery(players).foreach(selectAllQuery.insertValue)
    protected inline def deleteAllQuery = selectAllQuery.delete
    protected inline def deleteQuery(playerId: PlayerId): Delete[Player] = selectIdQuery(playerId).delete

    def selectAll: SqlTask[List[Player]]
    def selectId(playerId: PlayerId): SqlTask[Option[Player]]
    def insert(player: Player): SqlTask[Unit]
    def insertBatch(players: Iterable[Player]): SqlTask[Unit]
    def deleteAll: SqlTask[Unit]
    def deleteId(playerId: PlayerId): SqlTask[Unit]
  }

  private case class PostgresRepo(quill: Quill.Postgres[SnakeCase]) extends PlayerRepository {
    import quill.*

    override def selectAll: SqlTask[List[Player]] = run(selectAllQuery)
    override def selectId(playerId: PlayerId): SqlTask[Option[Player]] = run(selectIdQuery(playerId)).map(_.headOption)
    override def insert(player: Player): SqlTask[Unit] = run(insertQuery(player)).unit
    override def insertBatch(players: Iterable[Player]): SqlTask[Unit] = run(insertBatchQuery(players)).unit
    override def deleteAll: SqlTask[Unit] = run(deleteAllQuery).unit
    override def deleteId(playerId: PlayerId): SqlTask[Unit] = run(deleteQuery(playerId)).unit
  }

  private case class SqliteRepo(quill: Quill.Sqlite[SnakeCase]) extends PlayerRepository {
    import quill.*

    override def selectAll: SqlTask[List[Player]] = run(selectAllQuery)
    override def selectId(playerId: PlayerId): SqlTask[Option[Player]] = run(selectIdQuery(playerId)).map(_.headOption)
    override def insert(player: Player): SqlTask[Unit] = run(insertQuery(player)).unit
    override def insertBatch(players: Iterable[Player]): SqlTask[Unit] = run(insertBatchQuery(players)).unit
    override def deleteAll: SqlTask[Unit] = run(deleteAllQuery).unit
    override def deleteId(playerId: PlayerId): SqlTask[Unit] = run(deleteQuery(playerId)).unit
  }
}
