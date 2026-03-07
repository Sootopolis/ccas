package ccas.analysis.tables

import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.enums.{PlayerStatusCategory, Title}
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.SqlRepoUtils
import io.getquill.*
import io.getquill.extras.InstantOps
import io.getquill.jdbczio.Quill

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

  override protected def makeRepo(quill: Quill.Postgres[SnakeCase]): Repo = PlayerSnapshotRepository(quill)

  def selectAll: RepoTask[List[PlayerSnapshot]] = repoService(_.selectAll)
  def selectLatest: RepoTask[List[PlayerSnapshot]] = repoService(_.selectLatest)
  def selectActive: RepoTask[List[PlayerSnapshot]] = repoService(_.selectActive)
  def selectId(playerId: PlayerId): RepoTask[List[PlayerSnapshot]] = repoService(_.selectId(playerId))
  def selectName(username: Username): RepoTask[List[PlayerSnapshot]] = repoService(_.selectName(username))
  def selectIdLatest(playerId: PlayerId): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectIdLatest(playerId))
  def selectNameLatest(username: Username): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectNameLatest(username))
  def selectSince(since: Instant): RepoTask[List[PlayerSnapshot]] = repoService(_.selectSince(since))
  def insert(item: PlayerSnapshot): RepoTask[Unit] = repoService(_.insert(item))
  def insertBatch(items: Iterable[PlayerSnapshot]): RepoTask[Unit] = repoService(_.insertBatch(items))
  def update(item: PlayerSnapshot): RepoTask[Unit] = repoService(_.update(item))
  def updateBatch(items: Iterable[PlayerSnapshot]): RepoTask[Unit] = repoService(_.updateBatch(items))
  def deleteAll: RepoTask[Unit] = repoService(_.deleteAll)

  case class PlayerSnapshotRepository(quill: Quill.Postgres[SnakeCase]) {
    import quill.*

    inline def selectAllQuery = query[PlayerSnapshot]
    inline def selectLatestQuery = {
      selectAllQuery
        .join { query[PlayerSnapshot].groupByMap(_.playerId)(row => row.playerId -> max(row.since)) }
        .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }.map(_._1)
    }
    inline def selectActiveQuery = selectLatestQuery.filter(_.status == lift(Active))
    inline def selectIdQuery(playerId: PlayerId) = selectAllQuery.filter(_.playerId == lift(playerId))
    inline def selectNameQuery(username: Username) = selectAllQuery.filter(_.username == lift(username))
    inline def selectIdLatestQuery(playerId: PlayerId) = selectIdQuery(playerId).sortBy(_.since)(Ord.desc)
    inline def selectNameLatestQuery(username: Username) = selectNameQuery(username).sortBy(_.since)(Ord.desc)
    inline def selectSinceQuery(since: Instant) = {
      val justBeforeById = selectAllQuery.filter(_.since <= lift(since))
        .groupByMap(_.playerId)(x => x.playerId -> max(x.since))
      val justBefore = selectAllQuery.join(justBeforeById)
        .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }.map(_._1)
      val after = selectAllQuery.filter(_.since > lift(since))
      justBefore.union(after)
    }
    private inline def insertLifted(item: PlayerSnapshot): Insert[PlayerSnapshot] = selectAllQuery.insertValue(item)
    inline def insertQuery(item: PlayerSnapshot) = insertLifted(lift(item))
    inline def insertBatchQuery(items: Iterable[PlayerSnapshot]) = liftQuery(items).foreach(insertLifted)
    private inline def updateLifted(item: PlayerSnapshot): Update[PlayerSnapshot] =
      selectAllQuery.filter(_.playerId == item.playerId).filter(_.since == item.since).updateValue(item)
    inline def updateQuery(item: PlayerSnapshot) = updateLifted(lift(item))
    inline def updateBatchQuery(items: Iterable[PlayerSnapshot]) = liftQuery(items).foreach(updateLifted)
    inline def deleteAllQuery = selectAllQuery.delete

    def selectAll: SqlTask[List[PlayerSnapshot]] = run(selectAllQuery)
    def selectLatest: SqlTask[List[PlayerSnapshot]] = run(selectLatestQuery)
    def selectActive: SqlTask[List[PlayerSnapshot]] = run(selectActiveQuery)
    def selectId(id: PlayerId): SqlTask[List[PlayerSnapshot]] = run(selectIdQuery(id))
    def selectName(username: Username): SqlTask[List[PlayerSnapshot]] = run(selectNameQuery(username))
    def selectIdLatest(id: PlayerId): SqlTask[Option[PlayerSnapshot]] =
      run(selectIdLatestQuery(id)).map(_.headOption)
    def selectNameLatest(username: Username): SqlTask[Option[PlayerSnapshot]] =
      run(selectNameLatestQuery(username)).map(_.headOption)
    def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]] = run(selectSinceQuery(since))
    def insert(item: PlayerSnapshot): SqlTask[Unit] = run(insertQuery(item)).unit
    def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] = run(insertBatchQuery(items)).unit
    def update(item: PlayerSnapshot): SqlTask[Unit] = run(updateQuery(item)).unit
    def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] = run(updateBatchQuery(items)).unit
    def deleteAll: SqlTask[Unit] = run(deleteAllQuery).unit
  }
}
