package ccas.utils.sql

import io.getquill.SnakeCase
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.jdbczio.Quill
import zio.{RIO, Tag, TaskLayer, URLayer, ZIO, ZLayer}

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

  case class Postgres(quill: Quill.Postgres[SnakeCase]) extends QuillWrapper
  case class Sqlite(quill: Quill.Sqlite[SnakeCase]) extends QuillWrapper

  // TODO more wrappers here

  def liveFromPrefix(prefix: String = defaultDataSourcePrefix): TaskLayer[QuillWrapper] =
    Quill.DataSource.fromPrefix(prefix) >>> live

  private val live: ZLayer[DataSource, Throwable, QuillWrapper] = ZLayer.scoped {
    for {
      dataSource <- ZIO.service[DataSource]
      connection <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(dataSource.getConnection))
      rawDatabaseProductName <- ZIO.attempt(connection.getMetaData.getDatabaseProductName)
        .orElseFail(new Exception(s"No database product name found."))
      databaseProductName <- ZIO.attempt(DatabaseProduceName.valueOf(rawDatabaseProductName))
        .orElseFail(new Exception(s"Unsupported database product name: `$rawDatabaseProductName`"))
      wrapped <- databaseProductName match {
        case DatabaseProduceName.PostgreSQL => buildSnakeCase(Quill.Postgres.fromNamingStrategy, Postgres(_))
        case DatabaseProduceName.SQLite => buildSnakeCase(Quill.Sqlite.fromNamingStrategy, Sqlite(_))
      }
    } yield { wrapped }
  }

  private def buildSnakeCase[Q <: Quill[?, SnakeCase]: Tag](
    service: SnakeCase => URLayer[DataSource, Q],
    wrap   : Q => QuillWrapper
  ): RIO[DataSource, QuillWrapper] = ZIO.serviceWith[Q](wrap).provideSomeLayer(service(SnakeCase))
}
