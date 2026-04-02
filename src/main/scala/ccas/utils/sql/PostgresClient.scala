package ccas.utils.sql

import java.io.PrintWriter
import java.lang.reflect.Method
import java.sql.{Connection, SQLException}
import java.util.logging.Logger as JLogger
import javax.sql.DataSource

import com.augustnagro.magnum.{DbCon, DbTx, Transactor}
import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import zio.{durationLong, Duration, IO, RIO, Schedule, TaskLayer, ZIO, ZLayer}

/** Database client for PostgreSQL with connection management and transient-error retry.
  *
  * Wraps a Magnum `Transactor` (backed by HikariCP) and adds:
  *   - '''Connection pool hardening''' — keepalive probes, validation queries, and lazy initialization tolerant of cold
  *     starts (e.g. Neon serverless).
  *   - '''Transient-error retry''' — exponential backoff on connection-class errors (SQLState `08xxx`) so callers don't
  *     need to handle reconnection themselves.
  *   - '''Transaction support''' — `withTransaction` runs a multi-statement ZIO effect in a single JDBC transaction,
  *     retrying the entire transaction (not individual statements) on transient failure.
  *
  * Constructed via the `PostgresClient.live` ZLayer which reads configuration from `application.conf` under a
  * configurable prefix (default `"database"`).
  */
final class PostgresClient private (
  private[sql] val transactor: Transactor,
  private val retryBaseDelay: Duration,
  private val retryMaxRetries: Int
) {
  import PostgresClient.*

  private def retrySchedule[E <: Throwable]: Schedule[Any, E, Any] =
    Schedule.exponential(retryBaseDelay) &&
      Schedule.recurs(retryMaxRetries) &&
      Schedule.recurWhile[E](isTransient)

  /** Run a read-only block with a pooled connection, retrying on transient errors. */
  def connect[A](f: DbCon ?=> A): IO[SQLException, A] =
    ZIO
      .attempt(com.augustnagro.magnum.connect(transactor)(f))
      .refineToOrDie[SQLException]
      .retry(retrySchedule)

  /** Run a single-statement write in its own transaction, retrying on transient errors. */
  def transact[A](f: DbTx ?=> A): IO[SQLException, A] =
    ZIO
      .attempt(com.augustnagro.magnum.transact(transactor)(f))
      .refineToOrDie[SQLException]
      .retry(retrySchedule)

  /** Run a multi-statement ZIO effect within a single JDBC transaction.
    *
    * All `connectZIO` / `transactZIO` calls inside `f` share the same underlying connection. On success the transaction
    * is committed; on any failure or defect it is rolled back. The inner effect receives a non-retrying
    * `PostgresClient` so that individual statements do not retry independently (which would break atomicity). On
    * transient failure the *entire* transaction is retried from scratch.
    *
    * The `R` type parameter passes through any additional service requirements (e.g. `CcasLogger`) — only
    * `PostgresClient` is provided/eliminated by this method.
    */
  def withTransaction[R, E >: SQLException <: Throwable, A](
    f: ZIO[R & PostgresClient, E, A]
  ): ZIO[R, E, A] = {
    val attempt: ZIO[R, E, A] = ZIO.acquireReleaseWith(
      acquire = ZIO.attempt(transactor.dataSource.getConnection).refineToOrDie[SQLException]
    )(
      release = con => ZIO.succeed(con.close())
    ) { con =>
      con.setAutoCommit(false)
      val proxy    = transactionProxy(con)
      val scopedXa = transactor.copy(dataSource = SingleConnectionDataSource(proxy))
      val txClient = new PostgresClient(scopedXa, retryBaseDelay, retryMaxRetries = 0)
      f.provideSome[R](ZLayer.succeed(txClient))
        .foldCauseZIO(
          failure = cause => ZIO.attempt(con.rollback()).ignore *> ZIO.failCause(cause),
          success = a => ZIO.attempt(con.commit()).refineToOrDie[SQLException].as(a)
        )
    }
    attempt.retry(retrySchedule)
  }
}

object PostgresClient {

  /** Run a block of raw SQL with a pooled connection, as a ZIO effect. */
  def connectZIO[A](f: DbCon ?=> A): ZIO[PostgresClient, SQLException, A] =
    ZIO.serviceWithZIO[PostgresClient](_.connect(f))

  /** Run a block of raw SQL in a single-statement transaction, as a ZIO effect. */
  def transactZIO[A](f: DbTx ?=> A): ZIO[PostgresClient, SQLException, A] =
    ZIO.serviceWithZIO[PostgresClient](_.transact(f))

  /** Run a ZIO effect that uses `connectZIO` calls within a single JDBC transaction. Every `connectZIO` inside `f`
    * shares the same underlying connection. Commits on success, rolls back on failure or interruption.
    */
  def withTransaction[R, E >: SQLException <: Throwable, A](
    f: ZIO[R & PostgresClient, E, A]
  ): ZIO[R & PostgresClient, E, A] =
    ZIO.serviceWithZIO[PostgresClient](_.withTransaction(f))

  def live(
    prefix: String = "database",
    schema: Option[String] = None,
    onInit: RIO[PostgresClient, Unit] = ZIO.unit
  ): TaskLayer[PostgresClient] =
    ZLayer.scoped {
      for {
        triple <- ZIO.acquireRelease(
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
              if (poolConfig.hasPath("keepaliveTime"))
                hikariConfig.setKeepaliveTime(poolConfig.getLong("keepaliveTime"))
              if (poolConfig.hasPath("connectionTestQuery"))
                hikariConfig.setConnectionTestQuery(poolConfig.getString("connectionTestQuery"))
              if (poolConfig.hasPath("initializationFailTimeout"))
                hikariConfig.setInitializationFailTimeout(poolConfig.getLong("initializationFailTimeout"))
            }

            val baseDelay  = if (config.hasPath("retry.baseDelayMs")) config.getLong("retry.baseDelayMs") else 100L
            val maxRetries = if (config.hasPath("retry.maxRetries")) config.getInt("retry.maxRetries") else 3

            (new HikariDataSource(hikariConfig), baseDelay, maxRetries)
          }
        ) { case (ds, _, _) => ZIO.succeed(ds.close()) }
        (hikariDs, baseDelay, maxRetries) = triple
        client = new PostgresClient(Transactor(hikariDs), baseDelay.millis, maxRetries)
        _ <- onInit.provideEnvironment(zio.ZEnvironment(client))
      } yield client
    }

  /** Build a PostgresClient from a pre-existing DataSource. Unlike `live`, this does not manage the DataSource
    * lifecycle (no close on scope exit) and does not read configuration. Useful for testing with custom DataSource
    * wrappers or for bringing your own connection pool.
    */
  def fromDataSource(
    dataSource: javax.sql.DataSource,
    retryBaseDelay: Duration = 100.millis,
    retryMaxRetries: Int = 3
  ): PostgresClient =
    new PostgresClient(Transactor(dataSource), retryBaseDelay, retryMaxRetries)

  // --- Transient error detection ---

  private[sql] def isTransient(e: Throwable): Boolean = e match {
    case sql: SQLException =>
      Option(sql.getSQLState).exists(_.startsWith("08")) ||
        Option(sql.getMessage).exists { msg =>
          msg.contains("terminating connection") ||
          msg.contains("Connection is closed") ||
          msg.contains("This connection has been closed")
        }
    case _ => false
  }

  // --- Transaction internals ---

  /** Wraps a Connection in a Proxy that suppresses close/commit/rollback/setAutoCommit, so nested transactZIO calls
    * inside withTransaction cannot break the outer transaction.
    */
  private def transactionProxy(con: Connection): Connection =
    java.lang.reflect.Proxy
      .newProxyInstance(
        con.getClass.getClassLoader,
        Array(classOf[Connection]),
        (_, method: Method, args: Array[AnyRef]) =>
          method.getName match {
            case "close" | "commit" | "rollback" => ()
            case "setAutoCommit"                 => ()
            case _ if args == null               => method.invoke(con)
            case _                               => method.invoke(con, args*)
          }
      )
      .asInstanceOf[Connection] // safe: proxy implements Connection interface

  /** Minimal DataSource that always returns the same (proxied) connection. */
  private class SingleConnectionDataSource(con: Connection) extends DataSource {
    override def getConnection: Connection                                     = con
    override def getConnection(username: String, password: String): Connection = con
    override def getLogWriter: PrintWriter                                     = new PrintWriter(java.io.OutputStream.nullOutputStream)
    override def setLogWriter(out: PrintWriter): Unit                          = ()
    override def setLoginTimeout(seconds: Int): Unit                           = ()
    override def getLoginTimeout: Int                                          = 0
    override def getParentLogger: JLogger = JLogger.getLogger("SingleConnectionDataSource")
    override def unwrap[T](iface: Class[T]): T =
      if (iface.isInstance(this)) { iface.cast(this) }
      else { throw new SQLException(s"Cannot unwrap to ${iface.getName}") }
    override def isWrapperFor(iface: Class[?]): Boolean = iface.isInstance(this)
  }
}
