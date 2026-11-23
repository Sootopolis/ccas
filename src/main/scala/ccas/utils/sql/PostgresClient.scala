package ccas.utils.sql

import java.io.{ByteArrayOutputStream, OutputStream, PrintWriter}
import java.lang.reflect.{Method, Proxy}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.sql.{Connection, SQLException, SQLTransientConnectionException}
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger as JLogger
import javax.sql.DataSource

import com.augustnagro.magnum.{DbCon, DbTx}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import zio.*

import ccas.utils.config.Settings

/** Database client for PostgreSQL with connection management and transient-error retry.
  *
  * Runs Magnum statements on a `DataSource` (HikariCP in production) and adds:
  *   - '''Connection pool hardening''' — validation queries, socket timeouts, and lazy initialisation tolerant of cold
  *     starts (e.g. Neon serverless).
  *   - '''Transient-error retry''' — exponential backoff on connection-class errors (SQLState `08xxx`) so callers don't
  *     need to handle reconnection themselves.
  *   - '''Transaction support''' — `withTransaction` runs a multi-statement ZIO effect in a single JDBC transaction,
  *     retrying the entire transaction (not individual statements) on transient failure.
  *
  * Built by [[PostgresClient.live]] from the `database` section of [[Settings]], or by [[PostgresClient.make]] from a
  * [[DatabaseConfig]] a caller holds.
  *
  * @param addressOption where the pool connects, for naming it to an operator; see [[PostgresClient.databaseAddress]]
  * @param maxPoolSizeOption the most connections the pool opens, absent for a `DataSource` this didn't build
  */
final class PostgresClient private (
  private[sql] val dataSource: DataSource,
  val addressOption: Option[DatabaseAddress],
  val maxPoolSizeOption: Option[Int],
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
    ZIO.attemptBlockingInterrupt(com.augustnagro.magnum.connect(dataSource)(f))
      .mapError(unwrapSqlCause)
      .refineToOrDie[SQLException]
      .retry(retrySchedule)

  /** Run a single-statement write in its own transaction, retrying on transient errors. */
  def transact[A](f: DbTx ?=> A): IO[SQLException, A] =
    ZIO.attemptBlockingInterrupt(com.augustnagro.magnum.transact(dataSource)(f))
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
        conn <- checkOut
        _    <- ZIO.attemptBlocking(conn.setAutoCommit(false)).refineToOrDie[SQLException]
        proxy = transactionProxy(conn)
        txClient = new PostgresClient(
          dataSource = SingleConnectionDataSource(proxy),
          addressOption = addressOption,
          maxPoolSizeOption = maxPoolSizeOption,
          retryBaseDelay = retryBaseDelay,
          retryMaxRetries = 0
        )
        result <- insideTransaction.locally(true)(f).provideSomeLayer[R](ZLayer.succeed(txClient)).foldCauseZIO(
          failure = cause => ZIO.attemptBlocking(conn.rollback()).ignore *> ZIO.failCause(cause),
          success = a => ZIO.attemptBlocking(conn.commit()).refineToOrDie[SQLException].as(a)
        )
      } yield result
    }.retry(retrySchedule)

  // Interruptible, like `connect` and `transact` (#304). The release reaches the connection through `slot`, not the
  // acquired value: ZIO discards a connection handed over just as the interrupt lands, and nothing else would return
  // it to the pool.
  private def checkOut: ZIO[Scope, SQLException, Connection] =
    ZIO.suspendSucceed {
      val slot = new AtomicReference[Connection]
      ZIO.acquireReleaseInterruptible(
        ZIO.attemptBlockingInterrupt {
          val conn = dataSource.getConnection
          slot.set(conn)
          conn
        }.refineToOrDie[SQLException]
      )(ZIO.succeedBlocking(Option(slot.get).foreach(_.close())))
    }
}

object PostgresClient {

  /** True on a fiber running a [[withTransaction]] body, and on any fiber it forks. For code that must refuse to run
    * there: a network round trip would hold the transaction's connection and locks open for its whole length (#300).
    */
  private[ccas] val insideTransaction: FiberRef[Boolean] =
    Unsafe.unsafe(implicit u => FiberRef.unsafe.make(false))

  /** Run a block of raw SQL with a pooled connection, as a ZIO effect. */
  def connectZIO[A](f: DbCon ?=> A): ZIO[PostgresClient, SQLException, A] =
    ZIO.serviceWithZIO[PostgresClient](_.connect(f))

  /** Run a block of raw SQL in a single-statement transaction, as a ZIO effect. */
  def transactZIO[A](f: DbTx ?=> A): ZIO[PostgresClient, SQLException, A] =
    ZIO.serviceWithZIO[PostgresClient](_.transact(f))

  /** Run a ZIO effect that uses `connectZIO` calls within a single JDBC transaction. Every `connectZIO` inside `f`
    * shares the same underlying connection. Commits on success, rolls back on failure or interruption. `f` runs with
    * [[insideTransaction]] set, so `ChessComClient` refuses to be used inside it.
    */
  def withTransaction[R, E >: SQLException <: Throwable, A](
    f: ZIO[R & PostgresClient, E, A]
  ): ZIO[R & PostgresClient, E, A] =
    ZIO.serviceWithZIO[PostgresClient](_.withTransaction(f))

  /** Resolve the JDBC schema to set on the pool. An explicit, non-blank `live(schema = ...)` arg wins; otherwise fall
    * back to `dataSource.currentSchema`. Any blank value — from either source (e.g. `.env` ships `DB_SCHEMA=`, and
    * HOCON's `${?DB_SCHEMA}` treats set-but-empty as present) — is intentionally treated as "no schema", so the pool
    * uses Postgres' default `search_path` — matching the `database.url` branch, which never sets a schema.
    */
  private[sql] def resolveSchema(explicit: Option[String], configured: Option[String]): Option[String] =
    explicit.orElse(configured).map(_.trim).filter(_.nonEmpty)

  /** A JDBC URL with any embedded credentials lifted out of it, ready to hand to Hikari. */
  private[ccas] final case class ResolvedJdbcUrl(jdbcUrl: String, user: Option[String], password: Option[String])

  /** libpq-only connection parameters this does not carry over. pgjdbc keeps unrecognised query parameters in its
    * `Properties` and ignores them, so leaving them in would not break a connection — they are dropped so the URL
    * stays honest about what is actually in effect. Not exhaustive: libpq has many more parameters, and any of them
    * that pgjdbc does not recognise is silently inert whether or not it is listed here. These are the ones that appear
    * in the connection strings providers hand out. Note `connect_timeout` is libpq's spelling; pgjdbc's own equivalent
    * is `connectTimeout` (set from `pool.connectTimeoutSeconds`) and is unaffected.
    */
  private val LibpqOnlyParams = Set("connect_timeout", "target_session_attrs", "passfile", "gssencmode", "service")

  // libpq parameters carried over under pgjdbc's name, in place. Under `sslmode=require` pgjdbc checks no certificate,
  // so `channel_binding` is all that authenticates the server a provider's string names: ADR 0014.
  private val LibpqRenamedParams = Map("channel_binding" -> "channelBinding")

  /** Normalise a configured `database.url` into a JDBC URL plus separately-carried credentials.
    *
    * Accepts both the `jdbc:` form and the libpq URI every managed provider hands out
    * (`postgresql://user:pass@host/db`), which pgjdbc rejects on both scheme and userinfo credentials. Credentials
    * are lifted out of the URL for both forms, because Hikari echoes the `jdbcUrl` into its failure message.
    *
    * Percent-decoding differs by position on purpose — RFC 3986 for userinfo, `URLDecoder` for query values — and
    * unifying the two would silently change the password of an existing working config. That, and what passes
    * through untouched: `docs/adr/0014-accept-both-database-url-forms.md`.
    */
  private[ccas] def normalizeJdbcUrl(raw: String): Either[String, ResolvedJdbcUrl] = {
    val url     = raw.trim
    val lowered = url.toLowerCase
    val shapeHint =
      "expected a JDBC URL (jdbc:postgresql://host[:port]/db?user=…&password=…) " +
        "or a libpq URI (postgresql://user:pass@host[:port]/db)"

    def after(prefix: String): String = url.drop(prefix.length)

    if (url.isEmpty) Left(s"database.url is blank — $shapeHint")
    else if (lowered.startsWith("jdbc:postgresql://")) rewrite(rest = after("jdbc:postgresql://"), alreadyJdbc = true)
    else if (lowered.startsWith("jdbc:postgresql:")) {
      // pgjdbc's shorthand for a local database, `jdbc:postgresql:db`: no authority, but its query can carry a login.
      val (database, query) = splitQuery(after("jdbc:postgresql:"))
      Right(liftQueryCredentials(s"jdbc:postgresql:$database", query))
    } else if (lowered.startsWith("jdbc:")) Right(ResolvedJdbcUrl(url, None, None))
    else if (lowered.startsWith("postgresql://")) rewrite(rest = after("postgresql://"), alreadyJdbc = false)
    else if (lowered.startsWith("postgres://")) rewrite(rest = after("postgres://"), alreadyJdbc = false)
    else Left(s"database.url has an unrecognised scheme — $shapeHint")
  }

  /** Rebuild `<authority>[/db][?query]` (everything after `://`) as a credential-free `jdbc:postgresql://` URL.
    * `alreadyJdbc` says whether `rest` came from a JDBC URL, whose empty host pgjdbc accepts.
    */
  private def rewrite(rest: String, alreadyJdbc: Boolean): Either[String, ResolvedJdbcUrl] = {
    // Split the query off first: a password may legitimately contain '@' or '/', so searching the whole string for the
    // userinfo/host boundary would mis-split a URL whose query carries the credentials.
    val (beforeQuery, query) = splitQuery(rest)

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

    // A '?' then an '@' before the path read two ways, as a password holding a '?' or as a path-less URL with an '@' in
    // a query value, and the wrong reading puts part of the password in the URL. libpq takes the first; this refuses
    // both, as `backup-neon.sh` does, so neither leaks.
    if (rest.takeWhile(_ != '/').dropWhile(_ != '?').contains('@')) {
      Left(
        "database.url has a '?' before an '@', which reads two ways — " +
          "percent-encode a '?' in the password (%3F) or an '@' in a query value (%40)"
      )
    } else if (hostPort.isEmpty && !alreadyJdbc) {
      // pgjdbc parses an empty host (`jdbc:postgresql:///db`), so a JDBC URL keeps one. In a libpq URI it means a local
      // Unix socket, which a JDBC URL cannot express, so that one fails with a message that says so.
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

      // An explicit query parameter wins over userinfo: that is the value pgjdbc would have used for an already-working
      // `jdbc:` URL, so lifting it preserves the existing behaviour of every configuration that has one.
      val lifted = liftQueryCredentials(s"jdbc:postgresql://$hostPort$path", query)
      Right(lifted.copy(user = lifted.user.orElse(userInfoUser), password = lifted.password.orElse(userInfoPassword)))
    }
  }

  /** `s` split at its first `?`, into what precedes it and the query after it. */
  private def splitQuery(s: String): (String, String) =
    s.indexOf('?') match {
      case -1 => (s, "")
      case i  => (s.take(i), s.drop(i + 1))
    }

  /** `base` with `query`'s parameters appended, [[LibpqRenamedParams]] renamed, less [[LibpqOnlyParams]] and the
    * credentials, which it returns apart.
    */
  private def liftQueryCredentials(base: String, query: String): ResolvedJdbcUrl = {
    val params = query.split('&').toList.filter(_.nonEmpty).map { param =>
      val (key, value) = param.indexOf('=') match {
        case -1 => (param, "")
        case i  => (param.take(i), param.drop(i + 1))
      }
      (LibpqRenamedParams.getOrElse(key, key), value)
    }

    // Last occurrence wins, as in pgjdbc, which puts each token into a `Properties`, so a repeated key overwrites.
    val user     = params.collect { case ("user", v) => decodeQuery(v) }.lastOption
    val password = params.collect { case ("password", v) => decodeQuery(v) }.lastOption

    val kept = params.collect {
      case (k, v) if k != "user" && k != "password" && !LibpqOnlyParams.contains(k) => s"$k=$v"
    }

    ResolvedJdbcUrl(base + (if (kept.isEmpty) "" else kept.mkString("?", "&", "")), user, password)
  }

  /** RFC 3986 percent-decoding for userinfo — `+` is a literal plus, unlike in a query string. Decodes to bytes first
    * and converts once, so a multi-byte UTF-8 escape sequence (`%C3%A9`) round-trips instead of becoming two
    * mis-decoded characters.
    */
  private def decodePercent(s: String): String = {
    val bytes = new ByteArrayOutputStream(s.length)
    var i     = 0
    while (i < s.length) {
      val isEscape = s.charAt(i) == '%' && i + 2 < s.length && isHex(s.charAt(i + 1)) && isHex(s.charAt(i + 2))
      if (isEscape) {
        bytes.write(Integer.parseInt(s.substring(i + 1, i + 3), 16))
        i += 3
      } else {
        bytes.write(s.substring(i, i + 1).getBytes(StandardCharsets.UTF_8))
        i += 1
      }
    }
    new String(bytes.toByteArray, StandardCharsets.UTF_8)
  }

  private def isHex(c: Char): Boolean =
    (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')

  /** Query-value decoding, matching what pgjdbc applies to `?user=` / `?password=` (so `+` decodes to a space). */
  private def decodeQuery(s: String): String =
    URLDecoder.decode(s, StandardCharsets.UTF_8)

  /** The JDBC URL and credentials a pool configured by `config` connects with: `url` when set, else the `dataSource`
    * keys, all but `currentSchema` of which it then needs. Only then is `DB_PORT` read as a number, so a URL connects
    * whatever it holds.
    */
  private[ccas] def resolveConnection(config: DatabaseConfig): Either[String, ResolvedJdbcUrl] =
    config.url match {
      case Some(url) => normalizeJdbcUrl(url)
      case None =>
        val source = config.dataSource
        for {
          server   <- required(source.serverName, "serverName", "DB_HOST")
          port     <- portNumber(source)
          database <- required(source.databaseName, "databaseName", "DB_NAME")
          user     <- required(source.user, "user", "DB_USER")
          password <- required(source.password, "password", "DB_PASSWORD")
        } yield ResolvedJdbcUrl(s"jdbc:postgresql://$server:$port/$database", Some(user), Some(password))
    }

  private def required[A](value: Option[A], key: String, variable: String): Either[String, A] =
    value.toRight(s"database.dataSource.$key ($variable) is not set, and neither is database.url (DATABASE_URL)")

  // A blank DB_PORT counts as unset, as a blank DB_SCHEMA does.
  private def portNumber(source: DatabaseConfig.Source): Either[String, Int] =
    required(source.portNumber.map(_.trim).filter(_.nonEmpty), "portNumber", "DB_PORT").flatMap { port =>
      port.toIntOption.toRight(s"database.dataSource.portNumber (DB_PORT) is not a number: $port")
    }

  // One host, or a bracketed IPv6 literal, with an optional numeric port. Anything else in that position is a URL this
  // cannot read, and showing it might show part of a password that was never percent-encoded. One such password still
  // passes: `u:12/34?x@host/db` has an unescaped `/` before its `?`, so `normalizeJdbcUrl` reads host `u`, port `12`.
  private val HostPort = """(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+)(:\d+)?""".r

  /** The address of a resolved `jdbc:postgresql://` URL, without its query string or any userinfo. `None` for a
    * non-Postgres or host-less URL, or one whose host part is not a host.
    */
  private[ccas] def databaseAddress(jdbcUrl: String): Option[DatabaseAddress] = {
    // Parsed by hand, not by pgjdbc's `Driver.parseURL`: given a URL without a password, as every resolved one is, that
    // reads `~/.pgpass` and `pg_service.conf` and puts the password it finds there into its result.
    val prefix = "jdbc:postgresql://"
    Option.when(jdbcUrl.toLowerCase.startsWith(prefix))(jdbcUrl.drop(prefix.length)).flatMap { rest =>
      // Userinfo ends at the last '@', as in `rewrite`: a password may hold an unescaped '@' or '/'.
      val beforeQuery      = rest.takeWhile(_ != '?')
      val (hostPort, path) = beforeQuery.drop(beforeQuery.lastIndexOf('@') + 1).span(_ != '/')
      Option.when(hostPort.nonEmpty && hostPort.split(',').forall(HostPort.matches))(DatabaseAddress(hostPort, path))
    }
  }

  /** The address a pool configured by `config` connects to, resolved without building one: for asking before a boot
    * whether it is the right database. A `Left` where [[make]] would fail before connecting: a `database.url` that
    * doesn't parse, or `dataSource` keys missing when neither `DATABASE_URL` nor `DB_*` is set.
    */
  def configuredAddress(config: DatabaseConfig): Either[String, Option[DatabaseAddress]] =
    resolveConnection(config).map(resolved => databaseAddress(resolved.jdbcUrl))

  /** The pool [[Settings]]' `database` section describes: [[make]], with `onInit` given the settings too. */
  def live(
    schema: Option[String] = None,
    onInit: RIO[PostgresClient & Settings, Unit] = ZIO.unit
  ): RLayer[Settings, PostgresClient] =
    ZLayer.scoped {
      for {
        settings <- ZIO.service[Settings]
        config   <- DatabaseConfig.load
        client   <- make(config, schema, onInit.provideSomeEnvironment[PostgresClient](_.add(settings)))
      } yield client
    }

  /** The pool `config` describes, closed with the scope, after `onInit` has run with it. `schema`, when non-blank,
    * replaces `dataSource.currentSchema` ([[resolveSchema]]).
    */
  def make(
    config: DatabaseConfig,
    schema: Option[String],
    onInit: RIO[PostgresClient, Unit]
  ): RIO[Scope, PostgresClient] =
    for {
      resolved <- ZIO.fromEither(resolveConnection(config)).mapError(new IllegalArgumentException(_))
      hikariDs <- ZIO.fromAutoCloseable(
        ZIO.attemptBlocking(new HikariDataSource(hikariConfig(config, resolved, schema)))
      )
      client = new PostgresClient(
        dataSource = hikariDs,
        addressOption = databaseAddress(resolved.jdbcUrl),
        maxPoolSizeOption = Some(config.pool.maximumPoolSize),
        retryBaseDelay = config.retry.baseDelayMs.millis,
        retryMaxRetries = config.retry.maxRetries
      )
      _ <- onInit.provideEnvironment(ZEnvironment(client))
    } yield client

  private def hikariConfig(config: DatabaseConfig, resolved: ResolvedJdbcUrl, schema: Option[String]): HikariConfig = {
    val hikari = new HikariConfig()
    // Accepts both the JDBC and libpq URL forms, and carries any embedded credentials separately so they never reach
    // Hikari's `jdbcUrl` (which it echoes into driver-level failure messages). See `normalizeJdbcUrl`.
    hikari.setJdbcUrl(resolved.jdbcUrl)
    resolved.user.foreach(hikari.setUsername)
    resolved.password.foreach(hikari.setPassword)
    if (config.url.isEmpty) { resolveSchema(schema, config.dataSource.currentSchema).foreach(hikari.setSchema) }
    val pool = config.pool
    hikari.setMaximumPoolSize(pool.maximumPoolSize)
    hikari.setMinimumIdle(pool.minimumIdle)
    hikari.setConnectionTimeout(pool.connectionTimeout)
    hikari.setIdleTimeout(pool.idleTimeout)
    hikari.setMaxLifetime(pool.maxLifetime)
    hikari.setKeepaliveTime(pool.keepaliveTime)
    hikari.setConnectionTestQuery(pool.connectionTestQuery)
    hikari.setInitializationFailTimeout(pool.initializationFailTimeout)
    // pgjdbc's own settings, in SECONDS, for every connection in either branch: `connectionTimeout` bounds checkout only,
    // so without `socketTimeout` a connection dropped mid-query (Neon autosuspend, an LB reset with no RST) parks its
    // thread in `socket.read()` forever, and the transient-error retry never fires.
    hikari.addDataSourceProperty("socketTimeout", pool.socketTimeoutSeconds.toString)
    hikari.addDataSourceProperty("connectTimeout", pool.connectTimeoutSeconds.toString)
    hikari.addDataSourceProperty("tcpKeepAlive", pool.tcpKeepAlive.toString)
    hikari
  }

  /** Build a PostgresClient from a pre-existing DataSource. Unlike [[make]], this does not manage the DataSource
    * lifecycle (no close on scope exit) and knows neither its address nor its size. Useful for testing with custom
    * DataSource wrappers or for bringing your own connection pool.
    */
  def fromDataSource(
    dataSource: javax.sql.DataSource,
    retryBaseDelay: Duration,
    retryMaxRetries: Int
  ): PostgresClient =
    new PostgresClient(
      dataSource = dataSource,
      addressOption = None,
      maxPoolSizeOption = None,
      retryBaseDelay = retryBaseDelay,
      retryMaxRetries = retryMaxRetries
    )

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
    Proxy
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
    override def getLogWriter: PrintWriter            = new PrintWriter(OutputStream.nullOutputStream)
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
