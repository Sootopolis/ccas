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
    postgresSnake = PostgresRepo.apply,
    sqliteSnake = SqliteRepo.apply,
  )

  /** Select all player records. */
  def select: RepoTask[List[Player]] = repoService(_.select)
  /** Delete a player record. */
  def delete(playerId: PlayerId): RepoTask[Unit] = repoService(_.delete(playerId))

  sealed trait PlayerRepository {
    val quill: Quill[? <: SqlIdiom, SnakeCase]
    import quill.*

    protected inline def selectQuery: EntityQuery[Player] = query[Player]
    protected inline def deleteQuery(playerId: PlayerId): Delete[Player] =
      selectQuery.filter(_.playerId == lift(playerId)).delete

    def select: SqlTask[List[Player]]
    def delete(playerId: PlayerId): SqlTask[Unit]
  }

  private case class PostgresRepo(quill: Quill.Postgres[SnakeCase]) extends PlayerRepository {
    import quill.*

    override def select: SqlTask[List[Player]] = run(selectQuery)
    override def delete(playerId: PlayerId): SqlTask[Unit] = run(deleteQuery(playerId)).unit
  }

  private case class SqliteRepo(quill: Quill.Sqlite[SnakeCase]) extends PlayerRepository {
    import quill.*

    override def select: SqlTask[List[Player]] = run(selectQuery)
    override def delete(playerId: PlayerId): SqlTask[Unit] = run(deleteQuery(playerId)).unit
  }
}
