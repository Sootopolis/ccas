package ccas.utils.sql

import com.augustnagro.magnum.Transactor
import zio.{RIO, TaskLayer, ZIO}

object FreshSchemaLayer {
  private val schemaNamePattern = "^[a-z_][a-z0-9_]*$".r

  def apply(schema: String, onInit: RIO[Transactor, Unit] = ZIO.unit): TaskLayer[Transactor] = {
    val resetSchema: RIO[Transactor, Unit] = ZIO.serviceWithZIO[Transactor] { xa =>
      ZIO.attempt {
        require(schemaNamePattern.matches(schema), s"Invalid schema name: $schema")
        val conn = xa.dataSource.getConnection
        try {
          val stmt = conn.createStatement()
          try {
            stmt.execute(s"DROP SCHEMA IF EXISTS $schema CASCADE"): Unit
            stmt.execute(s"CREATE SCHEMA $schema"): Unit
          } finally stmt.close()
        } finally conn.close()
      }
    }
    DataSourceLayer.liveFromPrefix(schema = Some(schema), onInit = resetSchema *> onInit)
  }
}
