package ccas.utils.sql

import io.getquill.SnakeCase
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.jdbczio.Quill
import zio.{Scope, TaskLayer, ZIO, ZLayer}

import javax.sql.DataSource

sealed trait QuillWrapper {
  val quill: Quill[? <: SqlIdiom, SnakeCase]
}

object QuillWrapper {
  private val defaultDataSourcePrefix = "database"

  private enum DatabaseProduceName {
    case PostgreSQL
    case SQLite
  }

  case class PostgresQuillWrapper(quill: Quill.Postgres[SnakeCase]) extends QuillWrapper

  case class SqliteQuillWrapper(quill: Quill.Sqlite[SnakeCase]) extends QuillWrapper

  // TODO more wrappers here

  private def live: ZLayer[DataSource, Throwable, QuillWrapper] = ZLayer.scoped {
    {
      for {
        dataSource <- ZIO.service[DataSource]
        connection <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(dataSource.getConnection))
        rawDatabaseProductName <- ZIO.attempt(connection.getMetaData.getDatabaseProductName)
          .orElseFail(new Exception(s"No database product name found."))
        databaseProductName <- ZIO.attempt(DatabaseProduceName.valueOf(rawDatabaseProductName))
          .orElseFail(new Exception(s"Unsupported database product name: $rawDatabaseProductName"))
        wrapped <- databaseProductName match {
          case DatabaseProduceName.PostgreSQL => ZIO.serviceWith[Quill.Postgres[SnakeCase]](PostgresQuillWrapper(_))
          case DatabaseProduceName.SQLite => ZIO.serviceWith[Quill.Sqlite[SnakeCase]](SqliteQuillWrapper(_))
        }
      } yield { wrapped }
    }.provideSome[Scope & DataSource](
      Quill.Postgres.fromNamingStrategy(SnakeCase),
      Quill.Sqlite.fromNamingStrategy(SnakeCase)
    )
  }

  def liveFromPrefix(prefix: String = defaultDataSourcePrefix): TaskLayer[QuillWrapper] =
    ZLayer.make[QuillWrapper](Quill.DataSource.fromPrefix(prefix), live)
}
