package ccas.utils.sql

import io.getquill.dsl.DateOps
import io.getquill.{PostgresZioJdbcContext, SnakeCase, SqliteZioJdbcContext}
import zio.{ULayer, ZLayer}
import zio.http.URL

import java.sql.Types
import java.time.Instant

final class CcasSqlContext extends SqliteZioJdbcContext(SnakeCase)

object CcasSqlContext {
  def layer: ULayer[CcasSqlContext] = ZLayer.succeed(new CcasSqlContext)
}
