package ccas.analysis.tables.general

import ccas.api.misc.enums.{PlayerStatusCategory, Title}
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.QuillWrapper
import ccas.utils.sql.QuillWrapper.{PostgresQuillWrapper, SqliteQuillWrapper}
import ccas.utils.sql.SqlZioTypes.{SqlTask, SqlRIO}
import io.getquill.*
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.extras.InstantOps
import io.getquill.jdbczio.Quill
import zio.{RLayer, ZIO, ZLayer}

import java.time.Instant

case class PlayerSnapshot(
  playerId: PlayerId,
  since   : Instant,
  username: Username,
  status  : PlayerStatusCategory,
  title   : Option[Title]
)

object PlayerSnapshot {
  inline given UpdateMeta[PlayerSnapshot] = updateMeta(_.playerId, _.since)

  sealed trait PlayerSnapshotRepository {
    val quill: Quill[? <: SqlIdiom, ? <: NamingStrategy]
    import quill.*

    protected inline def selectQuery: EntityQuery[PlayerSnapshot] = query[PlayerSnapshot]

    protected inline def selectLatestQuery: Query[PlayerSnapshot] = selectQuery
      .join { query[PlayerSnapshot].groupByMap(_.playerId)(row => row.playerId -> max(row.since)) }
      .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }
      .map(_._1)

    protected inline def selectForIdQuery(playerId: PlayerId): EntityQuery[PlayerSnapshot] =
      selectQuery.filter(_.playerId == lift(playerId))

    protected inline def selectSpecificQuery(playerId: PlayerId, since: Instant) =
      selectForIdQuery(playerId).filter(_.since == lift(since))

    protected inline def selectLatestForIdQuery(playerId: PlayerId): Option[PlayerSnapshot] =
      selectForIdQuery(playerId).sortBy(_.since)(Ord.desc).value

    protected inline def selectSinceQuery(since: Instant): Query[PlayerSnapshot] = {
      val justBeforeById = selectQuery.filter(_._2 < lift(since))
        .groupByMap(_.playerId)(row => row.playerId -> max(row.since))
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
    def selectForId(playerId: PlayerId): SqlTask[List[PlayerSnapshot]]
    def selectLatestForId(playerId: PlayerId): SqlTask[Option[PlayerSnapshot]]
    def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]]
    def insert(item: PlayerSnapshot): SqlTask[Unit]
    def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit]
    def update(item: PlayerSnapshot): SqlTask[Unit]
    def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit]
    def delete(playerId: PlayerId, since: Instant): SqlTask[Unit]
  }

  private case class PostgresRepo(quill: Quill.Postgres[SnakeCase]) extends PlayerSnapshotRepository {
    import quill.*

    override inline def select: SqlTask[List[PlayerSnapshot]] =
      run(selectQuery)
    override inline def selectLatest: SqlTask[List[PlayerSnapshot]] =
      run(selectLatestQuery)
    override inline def selectForId(playerId: PlayerId): SqlTask[List[PlayerSnapshot]] =
      run(selectForIdQuery(playerId))
    override inline def selectLatestForId(playerId: PlayerId): SqlTask[Option[PlayerSnapshot]] =
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

  private case class SqliteRepo(quill: Quill.Sqlite[SnakeCase]) extends PlayerSnapshotRepository {
    import quill.*

    override inline def select: SqlTask[List[PlayerSnapshot]] =
      run(selectQuery)
    override inline def selectLatest: SqlTask[List[PlayerSnapshot]] =
      run(selectLatestQuery)
    override inline def selectForId(playerId: PlayerId): SqlTask[List[PlayerSnapshot]] =
      run(selectForIdQuery(playerId))
    override inline def selectLatestForId(playerId: PlayerId): SqlTask[Option[PlayerSnapshot]] =
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

  /** A ZLayer for the player snapshot repository in accordance with the serviced Quill. */
  val repo: RLayer[QuillWrapper, PlayerSnapshotRepository] = ZLayer.fromZIO {
    ZIO.serviceWithZIO[QuillWrapper] {
      case PostgresQuillWrapper(quill) => ZIO.succeed(PostgresRepo(quill))
      case SqliteQuillWrapper(quill) => ZIO.succeed(SqliteRepo(quill))
    }
  }

  private type PlayerSnapshotTask[+A] = SqlRIO[PlayerSnapshotRepository, A]

  /** Selects all player snapshot records. */
  def select: PlayerSnapshotTask[List[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.select)
  /** Selects the latest player snapshot records. */
  def selectLatest: PlayerSnapshotTask[List[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.selectLatest)
  /** Selects all player snapshot records for a given player. */
  def selectForId(playerId: PlayerId): PlayerSnapshotTask[List[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.selectForId(playerId))
  /** Selects the latest player snapshot record of a given player, if one exists. */
  def selectLatestForId(playerId: PlayerId): PlayerSnapshotTask[Option[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.selectLatestForId(playerId))
  /** Selects all player snapshot records since and immediately before a given timestamp. */
  def selectSince(since: Instant): PlayerSnapshotTask[List[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.selectSince(since))
  /** Inserts a player snapshot record. */
  def insert(item: PlayerSnapshot): PlayerSnapshotTask[Unit] =
    ZIO.serviceWithZIO(_.insert(item))
  /** Inserts a collection of player snapshot records. */
  def insertBatch(items: Iterable[PlayerSnapshot]): PlayerSnapshotTask[Unit] =
    ZIO.serviceWithZIO(_.insertBatch(items))
  /** Updates a player snapshot record. */
  def update(item: PlayerSnapshot): PlayerSnapshotTask[Unit] =
    ZIO.serviceWithZIO(_.update(item))
  /** Updates a collection of player snapshot records. */
  def updateBatch(items: Iterable[PlayerSnapshot]): PlayerSnapshotTask[Unit] =
    ZIO.serviceWithZIO(_.updateBatch(items))
  /** Deletes a player snapshot record. */
  def delete(playerId: PlayerId, since: Instant): PlayerSnapshotTask[Unit] =
    ZIO.serviceWithZIO(_.delete(playerId, since))
}
