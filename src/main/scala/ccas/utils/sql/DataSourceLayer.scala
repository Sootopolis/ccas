package ccas.utils.sql

import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import zio.{RIO, TaskLayer, ZIO, ZLayer}

object DataSourceLayer {
  private val defaultPrefix = "database"

  def liveFromPrefix(
    prefix: String = defaultPrefix,
    schema: Option[String] = None,
    onInit: RIO[Transactor, Unit] = ZIO.unit
  ): TaskLayer[Transactor] =
    ZLayer.scoped {
      for {
        hikariDs <- ZIO.acquireRelease(
          ZIO.attempt {
            val config       = ConfigFactory.load().getConfig(prefix)
            val hikariConfig = new HikariConfig()

            if (config.hasPath("url")) {
              hikariConfig.setJdbcUrl(config.getString("url"))
            } else {
              val dsConfig = config.getConfig("dataSource")
              hikariConfig.setJdbcUrl(
                s"jdbc:postgresql://${dsConfig.getString("serverName")}:${dsConfig.getInt("portNumber")}/${dsConfig.getString("databaseName")}"
              )
              hikariConfig.setUsername(dsConfig.getString("user"))
              hikariConfig.setPassword(dsConfig.getString("password"))
              hikariConfig.setSchema(schema.getOrElse(dsConfig.getString("currentSchema")))
            }

            if (config.hasPath("pool")) {
              val poolConfig = config.getConfig("pool")
              if (poolConfig.hasPath("maximumPoolSize"))
                hikariConfig.setMaximumPoolSize(poolConfig.getInt("maximumPoolSize"))
              if (poolConfig.hasPath("minimumIdle")) hikariConfig.setMinimumIdle(poolConfig.getInt("minimumIdle"))
              if (poolConfig.hasPath("connectionTimeout"))
                hikariConfig.setConnectionTimeout(poolConfig.getLong("connectionTimeout"))
              if (poolConfig.hasPath("idleTimeout")) hikariConfig.setIdleTimeout(poolConfig.getLong("idleTimeout"))
              if (poolConfig.hasPath("maxLifetime")) hikariConfig.setMaxLifetime(poolConfig.getLong("maxLifetime"))
            }

            new HikariDataSource(hikariConfig)
          }
        )(ds => ZIO.succeed(ds.close()))
        transactor = Transactor(hikariDs)
        _ <- onInit.provideEnvironment(zio.ZEnvironment(transactor))
      } yield transactor
    }
}
