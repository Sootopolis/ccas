package ccas.analysis.tables

import ccas.api.misc.enums.{PlayerStatusCategory, Title}
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.{RepoResolver, SqlRepoUtils}
import io.getquill.*
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.extras.InstantOps
import io.getquill.jdbczio.Quill
import zio.ZIO

import java.time.Instant

case class PlayerSnapshot(
  playerId: PlayerId,
  since   : Instant,
  username: Username,
  status  : PlayerStatusCategory,
  title   : Option[Title]
)

object PlayerSnapshot extends SqlRepoUtils {
  inline given UpdateMeta[PlayerSnapshot] = updateMeta(_.playerId, _.since)

  override protected type Repo = PlayerSnapshotRepository

  override protected val repoResolver: RepoResolver[Repo] = RepoResolver(
    postgresSnake = PostgresRepo.apply,
    sqliteSnake = SqliteRepo.apply,
  )

  /** Selects all player snapshot records. */
  def select: RepoTask[List[PlayerSnapshot]] = repoService(_.select)
  /** Selects the latest player snapshot records. */
  def selectLatest: RepoTask[List[PlayerSnapshot]] = repoService(_.selectLatest)
  /** Selects all player snapshot records for a given player. */
  def selectId(playerId: PlayerId): RepoTask[List[PlayerSnapshot]] = repoService(_.selectId(playerId))
  /** Selects the latest player snapshot record of a given player, if one exists. */
  def selectIdLatest(playerId: PlayerId): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectIdLatest(playerId))
  /** Selects all player snapshot records since and immediately before a given timestamp. */
  def selectSince(since: Instant): RepoTask[List[PlayerSnapshot]] = repoService(_.selectSince(since))
  /** Inserts a player snapshot record. */
  def insert(item: PlayerSnapshot): RepoTask[Unit] = repoService(_.insert(item))
  /** Inserts a collection of player snapshot records. */
  def insertBatch(items: Iterable[PlayerSnapshot]): RepoTask[Unit] = repoService(_.insertBatch(items))
  /** Updates a player snapshot record. */
  def update(item: PlayerSnapshot): RepoTask[Unit] = repoService(_.update(item))
  /** Updates a collection of player snapshot records. */
  def updateBatch(items: Iterable[PlayerSnapshot]): RepoTask[Unit] = repoService(_.updateBatch(items))
  /** Deletes a player snapshot record. */
  def delete(playerId: PlayerId, since: Instant): RepoTask[Unit] = repoService(_.delete(playerId, since))

  sealed trait PlayerSnapshotRepository {
    val quill: Quill[? <: SqlIdiom, SnakeCase]
    import quill.*

    protected inline def selectQuery: EntityQuery[PlayerSnapshot] = query[PlayerSnapshot]

    protected inline def selectLatestQuery: Query[PlayerSnapshot] = selectQuery
      .join { query[PlayerSnapshot].groupByMap(_.playerId)(row => row.playerId -> max(row.since)) }
      .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }.map(_._1)

    protected inline def selectForIdQuery(playerId: PlayerId): EntityQuery[PlayerSnapshot] =
      selectQuery.filter(_.playerId == lift(playerId))

    private inline def selectSpecificQuery(playerId: PlayerId, since: Instant): EntityQuery[PlayerSnapshot] =
      selectForIdQuery(playerId).filter(_.since == lift(since))

    protected inline def selectLatestForIdQuery(playerId: PlayerId): Option[PlayerSnapshot] =
      selectForIdQuery(playerId).sortBy(_.since)(Ord.desc).value

    protected inline def selectSinceQuery(since: Instant): Query[PlayerSnapshot] = {
      val justBeforeById = selectQuery.filter(_._2 < lift(since))
        .groupByMap(_.playerId)(x => x.playerId -> max(x.since))
      val justBefore = selectQuery.join(justBeforeById)
        .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }.map(_._1)
      val after = selectQuery.filter(_.since >= lift(since))
      justBefore.union(after)
    }

    protected inline def insertQuery(item: PlayerSnapshot): Insert[PlayerSnapshot] =
      selectQuery.insertValue(lift(item))

    protected inline def insertBatchQuery(items: Iterable[PlayerSnapshot]) =
      liftQuery(items).foreach(selectQuery.insertValue(_))

    protected inline def updateQuery(item: PlayerSnapshot) =
      selectSpecificQuery(item.playerId, item.since).updateValue(lift(item))

    protected inline def updateBatchQuery(items: Iterable[PlayerSnapshot]) = liftQuery(items).foreach { item =>
      selectQuery.filter(_.playerId == item.playerId).filter(_.since == item.since).updateValue(item)
    }

    protected inline def deleteQuery(playerId: PlayerId, since: Instant): Delete[PlayerSnapshot] =
      selectSpecificQuery(playerId, since).delete

    def select: SqlTask[List[PlayerSnapshot]]
    def selectLatest: SqlTask[List[PlayerSnapshot]]
    def selectId(playerId: PlayerId): SqlTask[List[PlayerSnapshot]]
    def selectIdLatest(playerId: PlayerId): SqlTask[Option[PlayerSnapshot]]
    def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]]
    def insert(item: PlayerSnapshot): SqlTask[Unit]
    def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit]
    def update(item: PlayerSnapshot): SqlTask[Unit]
    def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit]
    def delete(playerId: PlayerId, since: Instant): SqlTask[Unit]
  }

  private case class PostgresRepo(quill: Quill.Postgres[SnakeCase]) extends Repo {
    import quill.*

    override inline def select: SqlTask[List[PlayerSnapshot]] =
      run(selectQuery)
    override inline def selectLatest: SqlTask[List[PlayerSnapshot]] =
      run(selectLatestQuery)
    override inline def selectId(playerId: PlayerId): SqlTask[List[PlayerSnapshot]] =
      run(selectForIdQuery(playerId))
    override inline def selectIdLatest(playerId: PlayerId): SqlTask[Option[PlayerSnapshot]] =
      run(selectLatestForIdQuery(playerId))
    override inline def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]] =
      run(selectSinceQuery(since))
    override inline def insert(item: PlayerSnapshot): SqlTask[Unit] =
      run(insertQuery(item)).unit
    override inline def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] =
      run(insertBatchQuery(items)).unit
    override inline def update(item: PlayerSnapshot): SqlTask[Unit] =
      run(updateQuery(item)).unit
    override inline def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] =
      run(updateBatchQuery(items)).unit
    override inline def delete(playerId: PlayerId, since: Instant): SqlTask[Unit] =
      run(deleteQuery(playerId, since)).unit
  }

  private case class SqliteRepo(quill: Quill.Sqlite[SnakeCase]) extends Repo {
    import quill.*

    override inline def select: SqlTask[List[PlayerSnapshot]] =
      run(selectQuery)
    override inline def selectLatest: SqlTask[List[PlayerSnapshot]] =
      run(selectLatestQuery)
    override inline def selectId(playerId: PlayerId): SqlTask[List[PlayerSnapshot]] =
      run(selectForIdQuery(playerId))
    override inline def selectIdLatest(playerId: PlayerId): SqlTask[Option[PlayerSnapshot]] =
      run(selectLatestForIdQuery(playerId))
    override inline def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]] =
      run(selectSinceQuery(since))
    override inline def insert(item: PlayerSnapshot): SqlTask[Unit] =
      run(insertQuery(item)).unit
    override inline def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] =
      run(insertBatchQuery(items)).unit
    override inline def update(item: PlayerSnapshot): SqlTask[Unit] =
      run(updateQuery(item)).unit
    override inline def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] =
      run(updateBatchQuery(items)).unit
    override inline def delete(playerId: PlayerId, since: Instant): SqlTask[Unit] =
      run(deleteQuery(playerId, since)).unit
  }
}
