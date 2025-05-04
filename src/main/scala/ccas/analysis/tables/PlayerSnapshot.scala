package ccas.analysis.tables

import ccas.api.misc.enums.PlayerStatusCategory.Active
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
  /** Select records of currently active players. */
  def selectActive: RepoTask[List[PlayerSnapshot]] = repoService(_.selectActive)
  /** Select all player snapshot records for a given player by id. */
  def selectId(playerId: PlayerId): RepoTask[List[PlayerSnapshot]] = repoService(_.selectId(playerId))
  /** Select all player snapshot records for a given player by username. */
  def selectName(username: Username): RepoTask[List[PlayerSnapshot]] = repoService(_.selectName(username))
  /** Select the latest player snapshot record, if exists, of a given player by id. */
  def selectIdLatest(playerId: PlayerId): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectIdLatest(playerId))
  /** Select the latest player snapshot record, if exists, of a given player by username. */
  def selectNameLatest(username: Username): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectNameLatest(username))
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

    protected inline def selectAllQuery = query[PlayerSnapshot]
    protected inline def selectLatestQuery = {
      selectAllQuery
        .join { query[PlayerSnapshot].groupByMap(_.playerId)(row => row.playerId -> max(row.since)) }
        .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }.map(_._1)
    }
    protected inline def selectActiveQuery = selectLatestQuery.filter(_.status == lift(Active))
    protected inline def selectIdQuery(playerId: PlayerId) = selectAllQuery.filter(_.playerId == lift(playerId))
    protected inline def selectNameQuery(username: Username) = selectAllQuery.filter(_.username == lift(username))
    protected inline def selectIdLatestQuery(playerId: PlayerId) = selectIdQuery(playerId).sortBy(_.since)(Ord.desc)
    protected inline def selectNameLatestQuery(username: Username) = selectNameQuery(username).sortBy(_.since)(Ord.desc)
    protected inline def selectSinceQuery(since: Instant) = {
      val justBeforeById = selectAllQuery.filter(_.since <= lift(since))
        .groupByMap(_.playerId)(x => x.playerId -> max(x.since))
      val justBefore = selectAllQuery.join(justBeforeById)
        .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }.map(_._1)
      val after = selectAllQuery.filter(_.since > lift(since))
      justBefore.union(after)
    }
    private inline def insertLifted(item: PlayerSnapshot): Insert[PlayerSnapshot] = selectAllQuery.insertValue(item)
    protected inline def insertQuery(item: PlayerSnapshot) = insertLifted(lift(item))
    protected inline def insertBatchQuery(items: Iterable[PlayerSnapshot]) = liftQuery(items).foreach(insertLifted)
    private inline def updateLifted(item: PlayerSnapshot): Update[PlayerSnapshot] =
      selectAllQuery.filter(_.playerId == item.playerId).filter(_.since == item.since).updateValue(item)
    protected inline def updateQuery(item: PlayerSnapshot) = updateLifted(lift(item))
    protected inline def updateBatchQuery(items: Iterable[PlayerSnapshot]) = liftQuery(items).foreach(updateLifted)
    protected inline def deleteAllQuery = selectAllQuery.delete

    def selectAll: SqlTask[List[PlayerSnapshot]]
    def selectLatest: SqlTask[List[PlayerSnapshot]]
    def selectActive: SqlTask[List[PlayerSnapshot]]
    def selectId(id: PlayerId): SqlTask[List[PlayerSnapshot]]
    def selectName(username: Username): SqlTask[List[PlayerSnapshot]]
    def selectIdLatest(id: PlayerId): SqlTask[Option[PlayerSnapshot]]
    def selectNameLatest(username: Username): SqlTask[Option[PlayerSnapshot]]
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
    override def selectActive: SqlTask[List[PlayerSnapshot]] = run(selectActiveQuery)
    override def selectId(id: PlayerId): SqlTask[List[PlayerSnapshot]] = run(selectIdQuery(id))
    override def selectName(username: Username): SqlTask[List[PlayerSnapshot]] = run(selectNameQuery(username))
    override def selectIdLatest(id: PlayerId): SqlTask[Option[PlayerSnapshot]] =
      run(selectIdLatestQuery(id)).map(_.headOption)
    override def selectNameLatest(username: Username): SqlTask[Option[PlayerSnapshot]] =
      run(selectNameLatestQuery(username)).map(_.headOption)
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
    override def selectActive: SqlTask[List[PlayerSnapshot]] = run(selectActiveQuery)
    override def selectId(id: PlayerId): SqlTask[List[PlayerSnapshot]] = run(selectIdQuery(id))
    override def selectName(username: Username): SqlTask[List[PlayerSnapshot]] = run(selectNameQuery(username))
    override def selectIdLatest(id: PlayerId): SqlTask[Option[PlayerSnapshot]] =
      run(selectIdLatestQuery(id)).map(_.headOption)
    override def selectNameLatest(username: Username): SqlTask[Option[PlayerSnapshot]] =
      run(selectNameLatestQuery(username)).map(_.headOption)
    override def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]] = run(selectSinceQuery(since))
    override def insert(item: PlayerSnapshot): SqlTask[Unit] = run(insertQuery(item)).unit
    override def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] = run(insertBatchQuery(items)).unit
    override def update(item: PlayerSnapshot): SqlTask[Unit] = run(updateQuery(item)).unit
    override def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] = run(updateBatchQuery(items)).unit
    override def deleteAll: SqlTask[Unit] = run(deleteAllQuery).unit
  }
}
