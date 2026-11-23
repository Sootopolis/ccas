package ccas.utils.sql

import javax.sql.DataSource

import zio.{durationInt, RIO, Task, TaskLayer, ZIO, ZLayer}

import ccas.utils.config.Settings

/** Database configurations a test builds from the test `application.conf`'s, rather than from blocks of its own. */
object TestDatabases {

  /** The test config's `database` section, as a live pool in the suite reads it. */
  val configured: Task[DatabaseConfig] = DatabaseConfig.load.provide(Settings.live)

  /** A database that can never be reached, with credentials in its URL: for asserting what the server says about a
    * database it cannot reach, and that the credentials stay out of it. Port 1 refuses at once, and the pool is lazy,
    * so building it connects to nothing.
    */
  val unreachable: Task[DatabaseConfig] =
    configured.map { config =>
      config.copy(
        url = Some("postgresql://alice:hunter2@127.0.0.1:1/nowhere"),
        pool = config.pool.copy(connectionTimeout = 250L, initializationFailTimeout = -1L)
      )
    }

  /** A client over a test's own `dataSource`, retrying three times from 100 ms, for a test whose subject isn't the
    * retry.
    */
  def over(dataSource: DataSource): PostgresClient =
    PostgresClient.fromDataSource(dataSource, retryBaseDelay = 100.millis, retryMaxRetries = 3)

  /** A pool of [[unreachable]], with `onInit` run on it under the test settings, as [[PostgresClient.live]] runs it. */
  def unreachablePool(onInit: RIO[PostgresClient & Settings, Unit]): TaskLayer[PostgresClient] =
    Settings.live >>> ZLayer.scoped {
      for {
        settings <- ZIO.service[Settings]
        config   <- unreachable
        client <- PostgresClient.make(
          config,
          schema = None,
          onInit = onInit.provideSomeEnvironment[PostgresClient](_.add(settings))
        )
      } yield client
    }
}
