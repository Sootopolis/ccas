package ccas.utils.sql

import io.getquill.*
import zio.{ULayer, ZIO, ZLayer}

import java.sql.SQLException
import javax.sql.DataSource

sealed class CcasPostgresContext extends PostgresZioJdbcContext(SnakeCase)

object CcasPostgresContext {
  self =>
  private object CcasPostgresContextImpl extends CcasPostgresContext

  type CcasPostgresZIO[T] = ZIO[CcasPostgresContext & DataSource, SQLException, T]

  val layer: ULayer[CcasPostgresContext] = ZLayer.succeed(CcasPostgresContextImpl)
}
