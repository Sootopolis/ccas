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
    postgres = PostgresRepo.apply,
    sqlite = SqliteRepo.apply,
  )

  /** Select all player snapshot records. */
  def selectAll: RepoTask[List[PlayerSnapshot]] = repoService(_.selectAll)
  /** Select the latest player snapshot records. */
  def selectLatest: RepoTask[List[PlayerSnapshot]] = repoService(_.selectLatest)
  /** Select all player snapshot records for a given player by id. */
  def selectId(playerId: PlayerId): RepoTask[List[PlayerSnapshot]] = repoService(_.selectId(playerId))
  /** Select all player snapshot records for a given player by username. */
  def selectUsername(username: Username): RepoTask[List[PlayerSnapshot]] = repoService(_.selectUsername(username))
  /** Select the latest player snapshot record, if exists, of a given player by id. */
  def selectIdLatest(playerId: PlayerId): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectIdLatest(playerId))
  /** Select the latest player snapshot record, if exists, of a given player by username. */
  def selectUsernameLatest(username: Username): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectUsernameLatest(username))
  /** Select all player snapshot records since and immediately before a given timestamp. */
  def selectSince(since: Instant): RepoTask[List[PlayerSnapshot]] = repoService(_.selectSince(since))
  /** Insert a player snapshot record. */
  def insert(item: PlayerSnapshot): RepoTask[Unit] = repoService(_.insert(item))
  /** Insert a collection of player snapshot records. */
  def insertBatch(items: Iterable[PlayerSnapshot]): RepoTask[Unit] = repoService(_.insertBatch(items))
  /** Update a player snapshot record. */
  def update(item: PlayerSnapshot): RepoTask[Unit] = repoService(_.update(item))
  /** Update a collection of player snapshot records. */
  def updateBatch(items: Iterable[PlayerSnapshot]): RepoTask[Unit] = repoService(_.updateBatch(items))
  /** Delete all player snapshot records. */
  def deleteAll: RepoTask[Unit] = repoService(_.deleteAll)

  sealed trait PlayerSnapshotRepository {
    val quill: Quill[? <: SqlIdiom, SnakeCase]
    import quill.*

    protected inline def selectAllQuery: EntityQuery[PlayerSnapshot] = query[PlayerSnapshot]

    protected inline def selectLatestQuery: Query[PlayerSnapshot] = selectAllQuery
      .join { query[PlayerSnapshot].groupByMap(_.playerId)(row => row.playerId -> max(row.since)) }
      .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }.map(_._1)

    protected inline def selectIdQuery(playerId: PlayerId): EntityQuery[PlayerSnapshot] =
      selectAllQuery.filter(_.playerId == lift(playerId))

    protected inline def selectUsernameQuery(username: Username): EntityQuery[PlayerSnapshot] =
      selectAllQuery.filter(_.username == lift(username))

    protected inline def selectIdLatestQuery(playerId: PlayerId): Option[PlayerSnapshot] =
      selectIdQuery(playerId).sortBy(_.since)(Ord.desc).take(1).value

    protected inline def selectUsernameLatestQuery(username: Username): Option[PlayerSnapshot] =
      selectUsernameQuery(username).sortBy(_.since)(Ord.desc).take(1).value

    protected inline def selectSinceQuery(since: Instant): Query[PlayerSnapshot] = {
      val justBeforeById = selectAllQuery.filter(_.since <= lift(since))
        .groupByMap(_.playerId)(x => x.playerId -> max(x.since))
      val justBefore = selectAllQuery.join(justBeforeById)
        .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }.map(_._1)
      val after = selectAllQuery.filter(_.since > lift(since))
      justBefore.union(after)
    }

    protected inline def insertQuery(item: PlayerSnapshot): Insert[PlayerSnapshot] =
      selectAllQuery.insertValue(lift(item))

    protected inline def insertBatchQuery(items: Iterable[PlayerSnapshot]): BatchAction[Insert[PlayerSnapshot]] =
      liftQuery(items).foreach(selectAllQuery.insertValue(_))

    protected inline def updateQuery(item: PlayerSnapshot): Update[PlayerSnapshot] =
      selectIdQuery(item.playerId).filter(_.since == lift(item.since)).updateValue(lift(item))

    protected inline def updateBatchQuery(items: Iterable[PlayerSnapshot]): BatchAction[Update[PlayerSnapshot]] = {
      liftQuery(items).foreach { item =>
        selectAllQuery.filter(_.playerId == item.playerId).filter(_.since == item.since).updateValue(item)
      }
    }

    protected inline def deleteAllQuery: Delete[PlayerSnapshot] = selectAllQuery.delete

    def selectAll: SqlTask[List[PlayerSnapshot]]
    def selectLatest: SqlTask[List[PlayerSnapshot]]
    def selectId(id: PlayerId): SqlTask[List[PlayerSnapshot]]
    def selectUsername(username: Username): SqlTask[List[PlayerSnapshot]]
    def selectIdLatest(id: PlayerId): SqlTask[Option[PlayerSnapshot]]
    def selectUsernameLatest(username: Username): SqlTask[Option[PlayerSnapshot]]
    def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]]
    def insert(item: PlayerSnapshot): SqlTask[Unit]
    def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit]
    def update(item: PlayerSnapshot): SqlTask[Unit]
    def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit]
    def deleteAll: SqlTask[Unit]
  }

  private case class PostgresRepo(quill: Quill.Postgres[SnakeCase]) extends Repo {
    import quill.*

    override def selectAll: SqlTask[List[PlayerSnapshot]] = run(selectAllQuery)
    override def selectLatest: SqlTask[List[PlayerSnapshot]] = run(selectLatestQuery)
    override def selectId(id: PlayerId): SqlTask[List[PlayerSnapshot]] = run(selectIdQuery(id))
    override def selectUsername(username: Username): SqlTask[List[PlayerSnapshot]] = run(selectUsernameQuery(username))
    override def selectIdLatest(id: PlayerId): SqlTask[Option[PlayerSnapshot]] = run(selectIdLatestQuery(id))
    override def selectUsernameLatest(username: Username): SqlTask[Option[PlayerSnapshot]] = run(selectUsernameLatestQuery(username))
    override def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]] = run(selectSinceQuery(since))
    override def insert(item: PlayerSnapshot): SqlTask[Unit] = run(insertQuery(item)).unit
    override def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] = run(insertBatchQuery(items)).unit
    override def update(item: PlayerSnapshot): SqlTask[Unit] = run(updateQuery(item)).unit
    override def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] = run(updateBatchQuery(items)).unit
    override def deleteAll: SqlTask[Unit] = run(deleteAllQuery).unit
  }

  private case class SqliteRepo(quill: Quill.Sqlite[SnakeCase]) extends Repo {
    import quill.*

    override def selectAll: SqlTask[List[PlayerSnapshot]] = run(selectAllQuery)
    override def selectLatest: SqlTask[List[PlayerSnapshot]] = run(selectLatestQuery)
    override def selectId(id: PlayerId): SqlTask[List[PlayerSnapshot]] = run(selectIdQuery(id))
    override def selectUsername(username: Username): SqlTask[List[PlayerSnapshot]] = run(selectUsernameQuery(username))
    override def selectIdLatest(id: PlayerId): SqlTask[Option[PlayerSnapshot]] = run(selectIdLatestQuery(id))
    override def selectUsernameLatest(username: Username): SqlTask[Option[PlayerSnapshot]] = run(selectUsernameLatestQuery(username))
    override def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]] = run(selectSinceQuery(since))
    override def insert(item: PlayerSnapshot): SqlTask[Unit] = run(insertQuery(item)).unit
    override def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] = run(insertBatchQuery(items)).unit
    override def update(item: PlayerSnapshot): SqlTask[Unit] = run(updateQuery(item)).unit
    override def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] = run(updateBatchQuery(items)).unit
    override def deleteAll: SqlTask[Unit] = run(deleteAllQuery).unit
  }
}
