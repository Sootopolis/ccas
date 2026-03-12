package ccas.utils.sql

import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import org.postgresql.ds.PGSimpleDataSource
import zio.{TaskLayer, ZIO, ZLayer}

object DataSourceLayer {
  private val defaultPrefix = "database"

  def liveFromPrefix(prefix: String = defaultPrefix): TaskLayer[Transactor] =
    ZLayer
      .fromZIO {
        ZIO.attempt {
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
            ds.setCurrentSchema(dsConfig.getString("currentSchema"))
          }
          Transactor(ds)
        }
      }
}
