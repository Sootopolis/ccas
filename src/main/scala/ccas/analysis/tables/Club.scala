package ccas.analysis.tables

import ccas.api.misc.subtypes.{ClubId, ClubUrlName}
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.SqlRepoUtils
import io.getquill.*
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

  override protected def makeRepo(quill: Quill.Postgres[SnakeCase]): Repo = ClubRepository(quill)

  def selectAll: RepoTask[List[Club]] = repoService(_.selectAll)
  def selectId(clubId: ClubId): RepoTask[Option[Club]] = repoService(_.selectId(clubId))
  def upsert(club: Club): RepoTask[Unit] = repoService(_.upsert(club))
  def upsertBatch(clubs: Iterable[Club]): RepoTask[Unit] = repoService(_.upsertBatch(clubs))

  case class ClubRepository(quill: Quill.Postgres[SnakeCase]) {
    import quill.*

    inline def selectAllQuery = query[Club]
    inline def selectIdQuery(clubId: ClubId) = selectAllQuery.filter(_.clubId == lift(clubId))
    private inline def upsertLifted(club: Club): Insert[Club] =
      selectAllQuery.insertValue(club).onConflictUpdate(_.clubId)((t, e) => t.urlName -> e.urlName)
    inline def upsertQuery(club: Club) = upsertLifted(lift(club))
    inline def upsertBatchQuery(clubs: Iterable[Club]) = liftQuery(clubs).foreach(upsertLifted)

    def selectAll: SqlTask[List[Club]] = run(selectAllQuery)
    def selectId(clubId: ClubId): SqlTask[Option[Club]] = run(selectIdQuery(clubId)).map(_.headOption)
    def upsert(club: Club): SqlTask[Unit] = run(upsertQuery(club)).unit
    def upsertBatch(clubs: Iterable[Club]): SqlTask[Unit] = run(upsertBatchQuery(clubs)).unit
  }
}
