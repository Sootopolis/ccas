package ccas.utils.sql

import io.getquill.SnakeCase
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.jdbczio.Quill
import zio.{TaskLayer, ZIO, ZLayer}

import javax.sql.DataSource

sealed trait QuillWrapper {
  val quill: Quill[? <: SqlIdiom, SnakeCase]
}

object QuillWrapper {
  case class PostgresQuillWrapper(quill: Quill.Postgres[SnakeCase]) extends QuillWrapper

  case class SqliteQuillWrapper(quill: Quill.Sqlite[SnakeCase]) extends QuillWrapper

  // TODO more wrappers here

  private def live: ZLayer[DataSource, Throwable, QuillWrapper] = ZLayer.scoped {
    for {
      dataSource <- ZIO.service[DataSource]
      metadata <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(dataSource.getConnection)).map(_.getMetaData)
      wrapped <- metadata.getDatabaseProductName match {
        case "PostgreSQL" => Quill.Postgres.fromNamingStrategy(SnakeCase).build.map(_.get).map(PostgresQuillWrapper(_))
        case "Sqlite" => Quill.Sqlite.fromNamingStrategy(SnakeCase).build.map(_.get).map(SqliteQuillWrapper(_))
        case other => ZIO.fail(new Exception(s"Not supported database type: $other"))
      }
    } yield { wrapped }
  }

  def liveFromPrefix(prefix: String): TaskLayer[QuillWrapper] =
    ZLayer.make[QuillWrapper](Quill.DataSource.fromPrefix(prefix), live)
}
