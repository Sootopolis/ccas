package ccas.analysis.tables

import ccas.api.misc.subtypes.{ClubId, ClubUrlName}
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.{RepoResolver, SqlRepoUtils}
import io.getquill.*
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.jdbczio.Quill

import java.time.Instant

case class Club(
  clubId : ClubId,
  created: Instant,
  urlName: ClubUrlName,
)

object Club extends SqlRepoUtils {
  inline given UpdateMeta[Club] = updateMeta(_.clubId)

  override protected type Repo = ClubRepository
  override protected val repoResolver: RepoResolver[Repo] = RepoResolver(
    postgres = PostgresRepo.apply,
    sqlite = SqliteRepo.apply,
  )

  def selectAll: RepoTask[List[Club]] = repoService(_.selectAll)
  def selectId(clubId: ClubId): RepoTask[Option[Club]] = repoService(_.selectId(clubId))
  def upsert(club: Club): RepoTask[Unit] = repoService(_.upsert(club))
  def upsertBatch(clubs: Iterable[Club]): RepoTask[Unit] = repoService(_.upsertBatch(clubs))

  sealed trait ClubRepository {
    val quill: Quill[? <: SqlIdiom, SnakeCase]
    import quill.*

    protected inline def selectAllQuery = query[Club]
    protected inline def selectIdQuery(clubId: ClubId) = selectAllQuery.filter(_.clubId == lift(clubId))
    private inline def upsertLifted(club: Club): Insert[Club] =
      selectAllQuery.insertValue(club).onConflictUpdate(_.clubId)((t, e) => t.urlName -> e.urlName)
    protected inline def upsertQuery(club: Club) = upsertLifted(lift(club))
    protected inline def upsertBatchQuery(clubs: Iterable[Club]) = liftQuery(clubs).foreach(upsertLifted)

    def selectAll: SqlTask[List[Club]]
    def selectId(clubId: ClubId): SqlTask[Option[Club]]
    def upsert(club: Club): SqlTask[Unit]
    def upsertBatch(clubs: Iterable[Club]): SqlTask[Unit]
  }

  private case class PostgresRepo(quill: Quill.Postgres[SnakeCase]) extends Repo {
    import quill.*

    override def selectAll: SqlTask[List[Club]] = run(selectAllQuery)
    override def selectId(clubId: ClubId): SqlTask[Option[Club]] = run(selectIdQuery(clubId)).map(_.headOption)
    override def upsert(club: Club): SqlTask[Unit] = run(upsertQuery(club)).unit
    override def upsertBatch(clubs: Iterable[Club]): SqlTask[Unit] = run(upsertBatchQuery(clubs)).unit
  }

  private case class SqliteRepo(quill: Quill.Sqlite[SnakeCase]) extends Repo {
    import quill.*

    override def selectAll: SqlTask[List[Club]] = run(selectAllQuery)
    override def selectId(clubId: ClubId): SqlTask[Option[Club]] = run(selectIdQuery(clubId)).map(_.headOption)
    override def upsert(club: Club): SqlTask[Unit] = run(upsertQuery(club)).unit
    override def upsertBatch(clubs: Iterable[Club]): SqlTask[Unit] = run(upsertBatchQuery(clubs)).unit
  }
}
