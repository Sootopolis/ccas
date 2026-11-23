package ccas.utils.sql

import zio.{RIO, ZIO}
import zio.config.magnolia.DeriveConfig

import ccas.utils.config.Settings

/** The `database` section of `application.conf`, where its defaults live. `url`, when set, wins over `dataSource`,
  * whose fields a pool needs only without one: [[PostgresClient.resolveConnection]].
  */
final case class DatabaseConfig(
  url: Option[String],
  dataSource: DatabaseConfig.Source,
  pool: DatabaseConfig.Pool,
  retry: DatabaseConfig.Retry
)

object DatabaseConfig {

  /** The connection's parts, for a database given without a URL. `portNumber` is read as a number only then:
    * [[PostgresClient.resolveConnection]].
    */
  final case class Source(
    user: Option[String],
    password: Option[String],
    databaseName: Option[String],
    portNumber: Option[String],
    serverName: Option[String],
    currentSchema: Option[String]
  )

  /** Hikari's pool settings, and pgjdbc's socket settings, which apply to every connection. */
  final case class Pool(
    maximumPoolSize: Int,
    minimumIdle: Int,
    connectionTimeout: Long,
    idleTimeout: Long,
    maxLifetime: Long,
    keepaliveTime: Long,
    connectionTestQuery: String,
    initializationFailTimeout: Long,
    socketTimeoutSeconds: Int,
    connectTimeoutSeconds: Int,
    tcpKeepAlive: Boolean
  )

  /** The transient-error retry: [[PostgresClient]]. */
  final case class Retry(baseDelayMs: Long, maxRetries: Int)

  given DeriveConfig[Source]         = DeriveConfig.derived
  given DeriveConfig[Pool]           = DeriveConfig.derived
  given DeriveConfig[Retry]          = DeriveConfig.derived
  given DeriveConfig[DatabaseConfig] = DeriveConfig.derived

  val load: RIO[Settings, DatabaseConfig] = ZIO.serviceWithZIO[Settings](_.section[DatabaseConfig]("database"))
}
