package ccas.utils.sql

import java.io.PrintWriter
import java.lang.reflect.Method
import java.sql.{Connection, SQLException, SQLTransientConnectionException}
import java.util.logging.Logger as JLogger
import javax.sql.DataSource

import com.augustnagro.magnum.{DbCon, DbTx, Transactor}
import com.typesafe.config.{Config, ConfigFactory}
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

  // `attemptBlockingInterrupt` (not `attemptBlocking`) so ZIO interruption Thread-interrupts the JDBC worker: on
  // shutdown/cancel a fiber parked in Hikari's connection checkout aborts the wait promptly instead of blocking for the
  // full `connectionTimeout` (#193). An in-flight `socket.read()` still ignores the interrupt, but that is already
  // bounded by the driver `socketTimeout`.

  /** Run a read-only block with a pooled connection, retrying on transient errors. */
  def connect[A](f: DbCon ?=> A): IO[SQLException, A] =
    ZIO.attemptBlockingInterrupt(com.augustnagro.magnum.connect(transactor)(f))
      .mapError(unwrapSqlCause)
      .refineToOrDie[SQLException]
      .retry(retrySchedule)

  /** Run a single-statement write in its own transaction, retrying on transient errors. */
  def transact[A](f: DbTx ?=> A): IO[SQLException, A] =
    ZIO.attemptBlockingInterrupt(com.augustnagro.magnum.transact(transactor)(f))
      .mapError(unwrapSqlCause)
      .refineToOrDie[SQLException]
      .retry(retrySchedule)

  /** Run a multi-statement ZIO effect within a single JDBC transaction.
    *
    * All `connectZIO` / `transactZIO` calls inside `f` share the same underlying connection. On success the transaction
    * is committed; on any failure or defect it is rolled back. The inner effect receives a non-retrying
    * `PostgresClient` so that individual statements do not retry independently (which would break atomicity). On
    * transient failure the *entire* transaction is retried from scratch.
    *
    * The `R` type parameter passes through any additional service requirements (e.g. `ProgressDisplay`) — only
    * `PostgresClient` is provided/eliminated by this method.
    */
  def withTransaction[R, E >: SQLException <: Throwable, A](f: ZIO[R & PostgresClient, E, A]): ZIO[R, E, A] =
    ZIO.scoped[R] {
      for {
        conn <- ZIO.fromAutoCloseable {
          ZIO.attemptBlocking(transactor.dataSource.getConnection).refineToOrDie[SQLException]
        }
        _ <- ZIO.attemptBlocking(conn.setAutoCommit(false)).refineToOrDie[SQLException]
        proxy    = transactionProxy(conn)
        scopedXa = transactor.copy(dataSource = SingleConnectionDataSource(proxy))
        txClient = new PostgresClient(scopedXa, retryBaseDelay, retryMaxRetries = 0)
        result <- f.provideSomeLayer[R](ZLayer.succeed(txClient)).foldCauseZIO(
          failure = cause => ZIO.attemptBlocking(conn.rollback()).ignore *> ZIO.failCause(cause),
          success = a => ZIO.attemptBlocking(conn.commit()).refineToOrDie[SQLException].as(a)
        )
      } yield result
    }.retry(retrySchedule)
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

  /** Resolve the JDBC schema to set on the pool. An explicit, non-blank `live(schema = ...)` arg wins; otherwise fall
    * back to `dataSource.currentSchema` '''only if present'''. Any blank value — from either source (e.g. `.env` ships
    * `DB_SCHEMA=`, and HOCON's `${?DB_SCHEMA}` treats set-but-empty as present) — is intentionally treated as "no
    * schema", so the pool uses Postgres' default `search_path` — matching the `database.url` branch, which never sets a
    * schema. Reading the key with `hasPath` rather than a bare `getString` avoids a `ConfigException$Missing` crash
    * when `DB_SCHEMA` is unset.
    */
  private[sql] def resolveSchema(explicit: Option[String], dsConfig: Config): Option[String] =
    explicit
      .orElse(Option.when(dsConfig.hasPath("currentSchema"))(dsConfig.getString("currentSchema")))
      .map(_.trim)
      .filter(_.nonEmpty)

  /** A JDBC URL with any embedded credentials lifted out of it, ready to hand to Hikari. */
  private[ccas] final case class ResolvedJdbcUrl(jdbcUrl: String, user: Option[String], password: Option[String])

  /** libpq-only connection parameters that pgjdbc has no equivalent for. pgjdbc keeps unrecognised query parameters in
    * its `Properties` and ignores them, so leaving them in would not break a connection — they are dropped so the URL
    * stays honest about what is actually in effect. Not exhaustive: libpq has many more parameters, and any of them
    * that pgjdbc does not recognise is silently inert whether or not it is listed here. These are the ones that appear
    * in the connection strings providers hand out. Note `connect_timeout` is libpq's spelling; pgjdbc's own equivalent
    * is `connectTimeout` (set from `pool.connectTimeoutSeconds`) and is unaffected.
    */
  private val LibpqOnlyParams =
    Set("channel_binding", "connect_timeout", "target_session_attrs", "passfile", "gssencmode", "service")

  /** Normalise a configured `database.url` into a JDBC URL plus separately-carried credentials.
    *
    * Every managed Postgres provider (Neon, Heroku, Render, Supabase, Railway, …) hands out the '''libpq''' connection
    * URI from PostgreSQL's own docs — `postgresql://user:pass@host/db?sslmode=require` — while pgjdbc accepts neither
    * its scheme (it requires the `jdbc:` subprotocol) nor credentials in userinfo position (it requires `?user=` /
    * `?password=`). Pasting a provider URL verbatim therefore failed at boot with Hikari's opaque
    * `RuntimeException: Failed to get driver instance for jdbcUrl=…`, naming the driver rather than the URL shape. This
    * accepts both forms and converts.
    *
    * Credentials are always lifted out of the URL rather than left in it, for both accepted forms: Hikari echoes the
    * `jdbcUrl` into that failure message, so a password embedded in the URL lands in the log on any driver-level
    * failure. `hikariConfig.setUsername` / `setPassword` are equivalent to the query parameters as far as pgjdbc is
    * concerned, and keep the secret out of the string.
    *
    * Percent-decoding differs by position, deliberately: userinfo follows RFC 3986 (percent-escapes only, `+` is a
    * literal plus), while query values follow what pgjdbc itself does to them (`URLDecoder`, so `+` decodes to a
    * space). Decoding a lifted query credential any other way would silently change the password of an existing,
    * working `jdbc:` configuration.
    *
    * Non-Postgres `jdbc:` URLs pass through untouched, as does any `jdbc:` URL this cannot improve on. Returns `Left`
    * with an actionable, credential-free message for anything that is neither form.
    *
    * Scheme matching is case-insensitive (RFC 3986 §3.1); the rest of the URL is left in the case it was written.
    */
  private[ccas] def normalizeJdbcUrl(raw: String): Either[String, ResolvedJdbcUrl] = {
    val url       = raw.trim
    val lowered   = url.toLowerCase
    val shapeHint =
      "expected a JDBC URL (jdbc:postgresql://host[:port]/db?user=…&password=…) " +
        "or a libpq URI (postgresql://user:pass@host[:port]/db)"

    def after(prefix: String): String = url.drop(prefix.length)

    if (url.isEmpty) Left(s"database.url is blank — $shapeHint")
    else if (lowered.startsWith("jdbc:postgresql://")) rewrite(after("jdbc:postgresql://"), url, alreadyJdbc = true)
    else if (lowered.startsWith("jdbc:")) Right(ResolvedJdbcUrl(url, None, None))
    else if (lowered.startsWith("postgresql://")) rewrite(after("postgresql://"), url, alreadyJdbc = false)
    else if (lowered.startsWith("postgres://")) rewrite(after("postgres://"), url, alreadyJdbc = false)
    else Left(s"database.url has an unrecognised scheme — $shapeHint")
  }

  /** Rebuild `<authority>[/db][?query]` (everything after `://`) as a credential-free `jdbc:postgresql://` URL.
    * `original` is the URL `rest` came from, and `alreadyJdbc` says whether it was already in JDBC form — together they
    * let an input pgjdbc can handle but this cannot improve on pass through unchanged rather than be rejected.
    */
  private def rewrite(rest: String, original: String, alreadyJdbc: Boolean): Either[String, ResolvedJdbcUrl] = {
    // Split the query off first: a password may legitimately contain '@' or '/', so searching the whole string for the
    // userinfo/host boundary would mis-split a URL whose query carries the credentials.
    val (beforeQuery, query) = rest.indexOf('?') match {
      case -1 => (rest, "")
      case i  => (rest.take(i), rest.drop(i + 1))
    }

    // Last '@', not first: an unescaped '@' inside a password is common enough that libpq documents percent-encoding
    // it, and splitting at the last one keeps such a URL working instead of producing a nonsense host.
    val (userInfo, hostAndPath) = beforeQuery.lastIndexOf('@') match {
      case -1 => (None, beforeQuery)
      case i  => (Some(beforeQuery.take(i)), beforeQuery.drop(i + 1))
    }

    val (hostPort, path) = hostAndPath.indexOf('/') match {
      case -1 => (hostAndPath, "")
      case i  => (hostAndPath.take(i), hostAndPath.drop(i))
    }

    // An empty host is meaningful to pgjdbc — `jdbc:postgresql:///db` parses, taking host and port from its own
    // defaults — so an already-JDBC URL passes through untouched rather than being rejected for a shape the driver
    // accepts. The libpq spelling of the same thing means a local Unix-socket connection, which a JDBC URL cannot
    // express at all, so that one is worth failing on with a message that says so.
    if (hostPort.isEmpty && alreadyJdbc) Right(ResolvedJdbcUrl(original, None, None))
    else if (hostPort.isEmpty) {
      Left(
        "database.url has no host — a local-socket libpq URI (postgresql:///db) has no JDBC equivalent; " +
          "give a host, as in postgresql://user:pass@host[:port]/db"
      )
    } else {
      val (userInfoUser, userInfoPassword) = userInfo match {
        case None => (None, None)
        case Some(info) =>
          info.indexOf(':') match {
            case -1 => (Some(decodePercent(info)), None)
            case i  => (Some(decodePercent(info.take(i))), Some(decodePercent(info.drop(i + 1))))
          }
      }

      val params = query.split('&').toList.filter(_.nonEmpty).map { param =>
        param.indexOf('=') match {
          case -1 => (param, "")
          case i  => (param.take(i), param.drop(i + 1))
        }
      }

      // An explicit query parameter wins over userinfo: that is the value pgjdbc would have used for an already-working
      // `jdbc:` URL, so lifting it preserves the existing behaviour of every configuration that has one. Last
      // occurrence wins for the same reason — pgjdbc puts each token into a `Properties`, so a repeated key overwrites.
      val queryUser     = params.collect { case ("user", v) => decodeQuery(v) }.lastOption
      val queryPassword = params.collect { case ("password", v) => decodeQuery(v) }.lastOption

      val kept = params.collect {
        case (k, v) if k != "user" && k != "password" && !LibpqOnlyParams.contains(k) => s"$k=$v"
      }

      val jdbcUrl = s"jdbc:postgresql://$hostPort$path" + (if (kept.isEmpty) "" else kept.mkString("?", "&", ""))
      Right(ResolvedJdbcUrl(jdbcUrl, queryUser.orElse(userInfoUser), queryPassword.orElse(userInfoPassword)))
    }
  }

  /** RFC 3986 percent-decoding for userinfo — `+` is a literal plus, unlike in a query string. Decodes to bytes first
    * and converts once, so a multi-byte UTF-8 escape sequence (`%C3%A9`) round-trips instead of becoming two
    * mis-decoded characters.
    */
  private def decodePercent(s: String): String = {
    val bytes = new java.io.ByteArrayOutputStream(s.length)
    var i     = 0
    while (i < s.length) {
      val isEscape = s.charAt(i) == '%' && i + 2 < s.length && isHex(s.charAt(i + 1)) && isHex(s.charAt(i + 2))
      if (isEscape) {
        bytes.write(Integer.parseInt(s.substring(i + 1, i + 3), 16))
        i += 3
      } else {
        bytes.write(s.substring(i, i + 1).getBytes(java.nio.charset.StandardCharsets.UTF_8))
        i += 1
      }
    }
    new String(bytes.toByteArray, java.nio.charset.StandardCharsets.UTF_8)
  }

  private def isHex(c: Char): Boolean =
    (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')

  /** Query-value decoding, matching what pgjdbc applies to `?user=` / `?password=` (so `+` decodes to a space). */
  private def decodeQuery(s: String): String =
    java.net.URLDecoder.decode(s, java.nio.charset.StandardCharsets.UTF_8)

  def live(
    prefix: String = "database",
    schema: Option[String] = None,
    onInit: RIO[PostgresClient, Unit] = ZIO.unit
  ): TaskLayer[PostgresClient] =
    ZLayer.scoped {
      for {
        (hikariConfig, baseDelay, maxRetries) <- ZIO.attemptBlocking {
          val config       = ConfigFactory.load().getConfig(prefix)
          val hikariConfig = new HikariConfig()

          if (config.hasPath("url")) {
            // Accepts both the JDBC and libpq URL forms, and carries any embedded credentials separately so they never
            // reach Hikari's `jdbcUrl` (which it echoes into driver-level failure messages). See `normalizeJdbcUrl`.
            val resolved = normalizeJdbcUrl(config.getString("url")).fold(
              msg => throw new IllegalArgumentException(msg),
              identity
            )
            hikariConfig.setJdbcUrl(resolved.jdbcUrl)
            resolved.user.foreach(hikariConfig.setUsername)
            resolved.password.foreach(hikariConfig.setPassword)
          } else {
            val dsConfig = config.getConfig("dataSource")
            hikariConfig.setJdbcUrl(
              s"jdbc:postgresql://${dsConfig.getString("serverName")}:${dsConfig.getInt("portNumber")}/${dsConfig.getString("databaseName")}"
            )
            hikariConfig.setUsername(dsConfig.getString("user"))
            hikariConfig.setPassword(dsConfig.getString("password"))
            resolveSchema(schema, dsConfig).foreach(hikariConfig.setSchema)
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
            // Driver-level (pgjdbc) socket hardening. No Hikari setter covers these: `connectionTimeout` above bounds
            // pool *checkout*, never the in-query `socket.read()`. Without `socketTimeout`, a connection silently
            // dropped mid-query (Neon autosuspend, network blip, LB reset with no RST) parks the JDBC thread in
            // `socket.read()` forever and the transient-retry below never fires (no exception is ever thrown). Setting
            // them here applies to every connection regardless of host or whether the `url` / `dataSource` config
            // branch was taken — both build a jdbcUrl, so Hikari's DriverDataSource forwards these to pgjdbc.
            // `socketTimeout` / `connectTimeout` are in SECONDS for pgjdbc.
            if (poolConfig.hasPath("socketTimeoutSeconds"))
              hikariConfig.addDataSourceProperty("socketTimeout", poolConfig.getInt("socketTimeoutSeconds").toString)
            if (poolConfig.hasPath("connectTimeoutSeconds"))
              hikariConfig.addDataSourceProperty("connectTimeout", poolConfig.getInt("connectTimeoutSeconds").toString)
            if (poolConfig.hasPath("tcpKeepAlive"))
              hikariConfig.addDataSourceProperty("tcpKeepAlive", poolConfig.getBoolean("tcpKeepAlive").toString)
          }

          val baseDelay  = if (config.hasPath("retry.baseDelayMs")) config.getLong("retry.baseDelayMs") else 100L
          val maxRetries = if (config.hasPath("retry.maxRetries")) config.getInt("retry.maxRetries") else 3

          (hikariConfig, baseDelay, maxRetries)
        }
        hikariDs <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(new HikariDataSource(hikariConfig)))
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

  // --- Exception unwrapping ---

  /** Magnum wraps JDBC exceptions in its own `SqlException extends RuntimeException`, which causes
    * `refineToOrDie[SQLException]` to discard the real exception as a defect. This extracts the underlying
    * `SQLException` from the cause chain so it surfaces as a typed ZIO failure.
    */
  private def unwrapSqlCause(e: Throwable): Throwable = e match {
    case se: SQLException => se
    case _ =>
      e.getCause match {
        case se: SQLException => se
        case _                => e
      }
  }

  // --- Transient error detection ---

  private[sql] def isTransient(e: Throwable): Boolean = e match {
    // Hikari throws SQLTransientConnectionException when it can't hand out a connection within `connectionTimeout`
    // (pool exhausted, or the DB unreachable *right now*), and stamps it with the last connect failure's 08xxx state —
    // so it would otherwise match the 08xxx branch below and be retried. Retrying inside the same call just re-times-out
    // after another `connectionTimeout`, turning one write into N x connectionTimeout of blocking (#193). Fail fast on
    // the *type* (robust to Hikari message-wording changes, unlike a substring match); the caller — or the next
    // scheduled tick — retries later, outside this blocking cycle.
    case _: SQLTransientConnectionException => false
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
  private def transactionProxy(conn: Connection): Connection =
    java.lang.reflect.Proxy
      .newProxyInstance(
        conn.getClass.getClassLoader,
        Array(classOf[Connection]),
        (_, method: Method, args: Array[AnyRef]) =>
          method.getName match {
            case "close" | "commit" | "rollback" => ()
            case "setAutoCommit"                 => ()
            case _ if args == null               => method.invoke(conn)
            case _                               => method.invoke(conn, args*)
          }
      )
      .asInstanceOf[Connection] // safe: proxy implements Connection interface

  /** Minimal DataSource that always returns the same (proxied) connection. */
  private class SingleConnectionDataSource(conn: Connection) extends DataSource {
    override def getConnection: Connection                                     = conn
    override def getConnection(username: String, password: String): Connection = conn
    override def getLogWriter: PrintWriter            = new PrintWriter(java.io.OutputStream.nullOutputStream)
    override def setLogWriter(out: PrintWriter): Unit = ()
    override def setLoginTimeout(seconds: Int): Unit  = ()
    override def getLoginTimeout: Int                 = 0
    override def getParentLogger: JLogger             = JLogger.getLogger("SingleConnectionDataSource")
    override def unwrap[T](iface: Class[T]): T =
      if (iface.isInstance(this)) { iface.cast(this) }
      else { throw new SQLException(s"Cannot unwrap to ${iface.getName}") }
    override def isWrapperFor(iface: Class[?]): Boolean = iface.isInstance(this)
  }
}
