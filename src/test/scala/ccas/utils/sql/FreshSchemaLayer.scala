package ccas.utils.sql

import zio.{RIO, TaskLayer, ZIO}

object FreshSchemaLayer {
  private val schemaNamePattern = "^[a-z_][a-z0-9_]*$".r

  def apply(schema: String, onInit: RIO[PostgresClient, Unit] = ZIO.unit): TaskLayer[PostgresClient] = {
    val resetSchema: RIO[PostgresClient, Unit] = ZIO.serviceWithZIO[PostgresClient] { pgClient =>
      ZIO.attempt {
        require(schemaNamePattern.matches(schema), s"Invalid schema name: $schema")
        val conn = pgClient.transactor.dataSource.getConnection
        try {
          val stmt = conn.createStatement()
          try {
            stmt.execute(s"DROP SCHEMA IF EXISTS $schema CASCADE"): Unit
            stmt.execute(s"CREATE SCHEMA $schema"): Unit
          } finally stmt.close()
        } finally conn.close()
      }
    }
    PostgresClient.live(schema = Some(schema), onInit = resetSchema *> onInit)
  }
}
