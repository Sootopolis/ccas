package ccas.utils.sql

import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import org.postgresql.ds.PGSimpleDataSource
import zio.{TaskLayer, ZIO, ZLayer}

object DataSourceLayer {
  private val defaultPrefix = "database"

  private val schemaNamePattern = "^[a-z_][a-z0-9_]*$".r

  def liveFromPrefix(
    prefix: String = defaultPrefix,
    schema: Option[String] = None,
    onInit: ZIO[Transactor, Throwable, Unit] = ZIO.unit
  ): TaskLayer[Transactor] =
    ZLayer
      .fromZIO {
        for {
          xa <- ZIO.attempt {
            val config = ConfigFactory.load().getConfig(prefix)
            val ds     = new PGSimpleDataSource()
            if (config.hasPath("url")) {
              ds.setUrl(config.getString("url"))
            } else {
              val dsConfig = config.getConfig("dataSource")
              ds.setUser(dsConfig.getString("user"))
              ds.setPassword(dsConfig.getString("password"))
              ds.setDatabaseName(dsConfig.getString("databaseName"))
              ds.setPortNumbers(Array(dsConfig.getInt("portNumber")))
              ds.setServerNames(Array(dsConfig.getString("serverName")))
              ds.setCurrentSchema(schema.getOrElse(dsConfig.getString("currentSchema")))
            }
            Transactor(ds)
          }
          _ <- ZIO.foreachDiscard(schema) { s =>
            ZIO.attempt {
              require(schemaNamePattern.matches(s), s"Invalid schema name: $s")
              val conn = xa.dataSource.getConnection
              try {
                val stmt = conn.createStatement()
                stmt.execute(s"DROP SCHEMA IF EXISTS $s CASCADE")
                stmt.execute(s"CREATE SCHEMA $s")
                stmt.close()
              } finally conn.close()
            }
          }
          _ <- onInit.provideEnvironment(zio.ZEnvironment(xa))
        } yield xa
      }
}
