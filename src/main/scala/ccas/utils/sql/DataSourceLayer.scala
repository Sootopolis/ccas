package ccas.utils.sql

import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import org.postgresql.ds.PGSimpleDataSource
import zio.{TaskLayer, ZIO, ZLayer}

object DataSourceLayer {
  private val defaultPrefix = "database"

  def liveFromPrefix(prefix: String = defaultPrefix): TaskLayer[Transactor] =
    ZLayer.fromZIO {
      ZIO.attempt {
        val config   = ConfigFactory.load().getConfig(prefix)
        val dsConfig = config.getConfig("dataSource")
        val ds       = new PGSimpleDataSource()
        ds.setUser(dsConfig.getString("user"))
        ds.setPassword(dsConfig.getString("password"))
        ds.setDatabaseName(dsConfig.getString("databaseName"))
        ds.setPortNumbers(Array(dsConfig.getInt("portNumber")))
        ds.setServerNames(Array(dsConfig.getString("serverName")))
        ds.setCurrentSchema(dsConfig.getString("currentSchema"))
        Transactor(ds)
      }
    }
}
