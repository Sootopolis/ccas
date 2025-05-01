package ccas.analysis.tables.general

import ccas.api.misc.enums.{PlayerStatusCategory, Title}
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.QuillWrapper
import ccas.utils.sql.QuillWrapper.{PostgresQuillWrapper, SqliteQuillWrapper}
import ccas.utils.sql.SqlZioTypes.{SqlIO, SqlRIO}
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

    protected inline def selectSpecificQuery(playerId: PlayerId, since: Instant) = {
      selectQuery
        .filter(_.playerId == lift(playerId))
        .filter(_.since == lift(since))
    }

    protected inline def selectLatestQuery: Query[PlayerSnapshot] = {
      selectQuery
        .join { query[PlayerSnapshot].groupByMap(_.playerId)(row => row.playerId -> max(row.since)) }
        .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }
        .map(_._1)
    }

    protected inline def selectForIdQuery(playerId: PlayerId): EntityQuery[PlayerSnapshot] = {
      selectQuery
        .filter(_.playerId == lift(playerId))
    }

    protected inline def selectLatestForIdQuery(playerId: PlayerId): Option[PlayerSnapshot] = {
      selectForIdQuery(playerId)
        .sortBy(_.since)(Ord.desc)
        .value
    }

    protected inline def selectSinceQuery(since: Instant): Query[PlayerSnapshot] = {
      val justBefore = selectQuery
        .join(
          selectQuery
            .filter(_._2 < lift(since))
            .groupByMap(_.playerId)(row => row.playerId -> max(row.since))
        )
        .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }
        .map(_._1)
      val after = selectQuery
        .filter(_.since >= lift(since))
      justBefore.union(after)
    }

    protected inline def insertQuery(item: PlayerSnapshot) = {
      selectQuery
        .insertValue(lift(item))
    }

    protected inline def insertBatchQuery(items: Iterable[PlayerSnapshot]) = {
      liftQuery(items).foreach { item =>
        selectQuery
          .insertValue(item)
      }
    }

    protected inline def updateQuery(item: PlayerSnapshot) = {
      selectSpecificQuery(item.playerId, item.since)
        .updateValue(lift(item))
    }

    protected inline def updateBatchQuery(items: Iterable[PlayerSnapshot]) = {
      liftQuery(items).foreach { item =>
        selectQuery
          .filter(_.playerId == item.playerId)
          .filter(_.since == item.since)
          .updateValue(item)
      }
    }

    protected inline def deleteQuery(playerId: PlayerId, since: Instant): Delete[PlayerSnapshot] = {
      selectSpecificQuery(playerId, since)
        .delete
    }
    
    def select: SqlIO[List[PlayerSnapshot]]
    def selectLatest: SqlIO[List[PlayerSnapshot]]
    def selectForId(playerId: PlayerId): SqlIO[List[PlayerSnapshot]]
    def selectLatestForId(playerId: PlayerId): SqlIO[Option[PlayerSnapshot]]
    def selectSince(since: Instant): SqlIO[List[PlayerSnapshot]]
    def insert(item: PlayerSnapshot): SqlIO[Unit]
    def insertBatch(items: Iterable[PlayerSnapshot]): SqlIO[Unit]
    def update(item: PlayerSnapshot): SqlIO[Unit]
    def updateBatch(items: Iterable[PlayerSnapshot]): SqlIO[Unit]
    def delete(playerId: PlayerId, since: Instant): SqlIO[Unit]
  }

  private type PlayerSnapshotZIO[+A] = SqlRIO[PlayerSnapshotRepository, A]

  inline def repo: RLayer[QuillWrapper, PlayerSnapshotRepository] = ZLayer.fromZIO {
    ZIO.serviceWithZIO[QuillWrapper] {
      case PostgresQuillWrapper(quill) => ZIO.succeed(PostgresRepo(quill))
      case SqliteQuillWrapper(quill) => ZIO.succeed(SqliteRepo(quill))
    }
  }

  private case class PostgresRepo(quill: Quill.Postgres[SnakeCase]) extends PlayerSnapshotRepository {
    import quill.*

    override inline def select: SqlIO[List[PlayerSnapshot]] =
      run(selectQuery)
    override inline def selectLatest: SqlIO[List[PlayerSnapshot]] =
      run(selectLatestQuery)
    override inline def selectForId(playerId: PlayerId): SqlIO[List[PlayerSnapshot]] =
      run(selectForIdQuery(playerId))
    override inline def selectLatestForId(playerId: PlayerId): SqlIO[Option[PlayerSnapshot]] =
      run(selectLatestForIdQuery(playerId))
    override inline def selectSince(since: Instant): SqlIO[List[PlayerSnapshot]] =
      run(selectSinceQuery(since))
    override inline def insert(item: PlayerSnapshot): SqlIO[Unit] =
      run(insertQuery(item)).unit
    override inline def insertBatch(items: Iterable[PlayerSnapshot]): SqlIO[Unit] =
      run(insertBatchQuery(items)).unit
    override inline def update(item: PlayerSnapshot): SqlIO[Unit] =
      run(updateQuery(item)).unit
    override inline def updateBatch(items: Iterable[PlayerSnapshot]): SqlIO[Unit] =
      run(updateBatchQuery(items)).unit
    override inline def delete(playerId: PlayerId, since: Instant): SqlIO[Unit] =
      run(deleteQuery(playerId, since)).unit
  }

  private case class SqliteRepo(quill: Quill.Sqlite[SnakeCase]) extends PlayerSnapshotRepository {
    import quill.*

    override inline def select: SqlIO[List[PlayerSnapshot]] =
      run(selectQuery)
    override inline def selectLatest: SqlIO[List[PlayerSnapshot]] =
      run(selectLatestQuery)
    override inline def selectForId(playerId: PlayerId): SqlIO[List[PlayerSnapshot]] =
      run(selectForIdQuery(playerId))
    override inline def selectLatestForId(playerId: PlayerId): SqlIO[Option[PlayerSnapshot]] =
      run(selectLatestForIdQuery(playerId))
    override inline def selectSince(since: Instant): SqlIO[List[PlayerSnapshot]] =
      run(selectSinceQuery(since))
    override inline def insert(item: PlayerSnapshot): SqlIO[Unit] =
      run(insertQuery(item)).unit
    override inline def insertBatch(items: Iterable[PlayerSnapshot]): SqlIO[Unit] =
      run(insertBatchQuery(items)).unit
    override inline def update(item: PlayerSnapshot): SqlIO[Unit] =
      run(updateQuery(item)).unit
    override inline def updateBatch(items: Iterable[PlayerSnapshot]): SqlIO[Unit] =
      run(updateBatchQuery(items)).unit
    override inline def delete(playerId: PlayerId, since: Instant): SqlIO[Unit] =
      run(deleteQuery(playerId, since)).unit
  }

  def select: PlayerSnapshotZIO[List[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.select)
  def selectLatest: PlayerSnapshotZIO[List[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.selectLatest)
  def selectForId(playerId: PlayerId): PlayerSnapshotZIO[List[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.selectForId(playerId))
  def selectLatestForId(playerId: PlayerId): PlayerSnapshotZIO[Option[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.selectLatestForId(playerId))
  def selectSince(since: Instant): PlayerSnapshotZIO[List[PlayerSnapshot]] =
    ZIO.serviceWithZIO(_.selectSince(since))
  def insert(item: PlayerSnapshot): PlayerSnapshotZIO[Unit] =
    ZIO.serviceWithZIO(_.insert(item))
  def insertBatch(items: Iterable[PlayerSnapshot]): PlayerSnapshotZIO[Unit] =
    ZIO.serviceWithZIO(_.insertBatch(items))
  def update(item: PlayerSnapshot): PlayerSnapshotZIO[Unit] =
    ZIO.serviceWithZIO(_.update(item))
  def updateBatch(items: Iterable[PlayerSnapshot]): PlayerSnapshotZIO[Unit] =
    ZIO.serviceWithZIO(_.updateBatch(items))
  def delete(playerId: PlayerId, since: Instant): PlayerSnapshotZIO[Unit] =
    ZIO.serviceWithZIO(_.delete(playerId, since))
}
