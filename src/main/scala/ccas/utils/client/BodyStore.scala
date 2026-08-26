package ccas.utils.client

import java.net.URI
import java.nio.file.{Path, Paths}

import com.typesafe.config.ConfigFactory
import zio.{Duration, Ref, RLayer, Scope, Task, UIO, URIO, ZIO, ZLayer}
import zio.config.derivation.kebabCase
import zio.config.magnolia.DeriveConfig
import zio.config.typesafe.TypesafeConfigProvider

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

import ccas.utils.errors.safeMessage

/** Content-addressed store for cached Chess.com response bodies, keyed by the hex SHA-256 already computed in
  * [[ccas.analysis.tables.ApiResponseBody]] — so `put` is idempotent and dedup is preserved for free.
  *
  * The trait is honest about I/O (`Task`); the degradation policy lives in the companion's accessors
  * ([[BodyStore.read]] / [[BodyStore.putOrSkip]]), which is what persistence code calls. The invariant those
  * enforce — the cache is not source of truth, so an outage takes it offline and never the app, and a merely slow
  * store must degrade identically — is `docs/adr/0008-body-store-outside-postgres.md` (#191, #200) and
  * `docs/adr/0009-bound-every-body-store-operation.md` (#211).
  */
trait BodyStore {
  def get(hash: String): Task[Option[Array[Byte]]]
  def put(hash: String, bytes: Array[Byte]): Task[Unit]
  def delete(hash: String): Task[Unit]
}

/** Outcome of a [[BodyStore]] read once the store's own failures have been absorbed.
  *
  * `Missing` and `Unavailable` are one `None` at the trait level but demand different repairs upstream: a missing
  * object means the `api_response_cache` row is a lie and must be dropped; an unavailable store means the row is
  * still good and must be kept. See `docs/adr/0008-body-store-outside-postgres.md` (#215). The backends already
  * draw the line (`NoSuchFileException` / `NoSuchKeyException` map to `None`); this type stops the accessor
  * throwing it away again.
  */
enum BodyRead[+A] {
  case Found(value: A)
  case Missing
  case Unavailable

  def map[B](f: A => B): BodyRead[B] = this match {
    case Found(value) => Found(f(value))
    case Missing      => Missing
    case Unavailable  => Unavailable
  }

  /** For callers where the distinction genuinely does not change what happens next — a diagnostic display, not a
    * cache repair. Cache code should pattern-match instead.
    */
  def toOption: Option[A] = this match {
    case Found(value) => Some(value)
    case Missing      => None
    case Unavailable  => None
  }
}

object BodyStore {

  // Service accessors so table code can require `BodyStore` from the ZIO environment alongside `PostgresClient`,
  // provided once at each callsite via `ZEnvironment(pgClient, bodyStore)`. Reads and writes are deliberately
  // exposed ONLY in their error-degrading form — there is no raw `get`/`put` accessor to reach for by mistake.

  /** Read a body's bytes, absorbing a store error or a [[Deadlines]] breach into [[BodyRead.Unavailable]], kept
    * distinct from the [[BodyRead.Missing]] a genuinely absent object produces — see [[BodyRead]].
    *
    * Interruption still propagates (`catchAll` covers typed failures only), so shutdown is unaffected.
    * Per-operation failures log at DEBUG: an outage fails every read on a fetch-heavy run, and the operator-facing
    * signal is [[HealthLogging]]'s single transition WARN.
    */
  def read(hash: String): URIO[BodyStore, BodyRead[Array[Byte]]] =
    ZIO
      .serviceWithZIO[BodyStore](_.get(hash))
      .map {
        case Some(bytes) => BodyRead.Found(bytes)
        case None        => BodyRead.Missing
      }
      .catchAll { e =>
        ZIO
          .logDebug(s"BodyStore read failed for $hash, treating as a cache miss: ${e.safeMessage}")
          .as(BodyRead.Unavailable)
      }

  /** Store a body's bytes, returning `false` when the store rejected the write.
    *
    * Callers must NOT write a hash-pointer row on `false`: a pointer with no object behind it is a cache entry that
    * can never be served, so it costs a Postgres row and buys nothing. Skipping the whole cache write instead means
    * an outage degrades to "no cache" and self-heals on the first successful put. #199's budget guard reuses this
    * exact skip.
    *
    * `source` names what the body came from — a content-addressed store can otherwise report only a hash.
    * DEBUG-level per-failure logging, for the same reason as [[read]].
    *
    * A write abandoned at its [[Deadlines]] budget may still land its object, stranding one no sweep can ever see.
    * That leak, and the age-based bucket lifecycle rule that is its only sound mitigation:
    * `docs/adr/0009-bound-every-body-store-operation.md`.
    */
  def putOrSkip(hash: String, bytes: Array[Byte], source: String): URIO[BodyStore, Boolean] =
    ZIO.serviceWithZIO[BodyStore](_.put(hash, bytes)).as(true).catchAll { e =>
      ZIO
        .logDebug(
          s"BodyStore write failed for $source ($hash, ${bytes.length} bytes), body not cached: ${e.safeMessage}"
        )
        .as(false)
    }

  /** Delete an object. Left in raw `Task` form because every callsite is already a best-effort `.ignore` — orphan
    * cleanup failing just leaves a harmless content-addressed object behind.
    */
  def delete(hash: String): ZIO[BodyStore, Throwable, Unit] =
    ZIO.serviceWithZIO[BodyStore](_.delete(hash))

  /** Raised by [[Deadlines]] when an operation outruns its budget. Named rather than a bare `TimeoutException` so a
    * later latency breaker can match on it.
    *
    * `bytes` is populated for a write only: size separates "too big to upload inside the budget" from "slow
    * regardless of size" (#222). No stack trace — this is control flow on the hot read path, not a defect, and the
    * frames would name the decorator.
    */
  final class BodyStoreTimeoutException(val op: String, val limit: Duration, val bytes: Option[Int])
      extends RuntimeException(BodyStoreTimeoutException.message(op, limit, bytes)) {
    override def fillInStackTrace(): Throwable = this
  }

  private object BodyStoreTimeoutException {
    private def message(op: String, limit: Duration, bytes: Option[Int]): String =
      bytes match {
        case Some(count) => s"BodyStore $op of $count bytes exceeded its ${limit.toMillis}ms deadline"
        case None        => s"BodyStore $op exceeded its ${limit.toMillis}ms deadline"
      }
  }

  private[ccas] val DefaultReadTimeoutMs      = 5000
  private[ccas] val DefaultWriteTimeoutMs     = 10000
  private[ccas] val DefaultS3ConnectTimeoutMs = 2000
  private[ccas] val DefaultS3SocketTimeoutMs  = 5000

  /** Per-operation wall-clock budgets, from `body-store.{read,write}-timeout-ms`. Reads get the tighter one;
    * writes are generous because an abandoned write manufactures the orphan objects [[putOrSkip]] documents.
    *
    * Both defaults are deliberately loose — set below the store's true p99 and the cache becomes an amplifier of
    * Chess.com load. Tighten from measurement, not argument:
    * `docs/adr/0009-bound-every-body-store-operation.md` (#211).
    */
  private[ccas] final case class BodyStoreLimits(read: Duration, write: Duration)

  private[ccas] def limitsFrom(config: BodyStoreConfig): Either[String, BodyStoreLimits] =
    for {
      read  <- positiveMs(config.readTimeoutMs, DefaultReadTimeoutMs, "body-store.read-timeout-ms")
      write <- positiveMs(config.writeTimeoutMs, DefaultWriteTimeoutMs, "body-store.write-timeout-ms")
    } yield BodyStoreLimits(read, write)

  /** The S3 transport's own budget, derived from the accessor deadlines: `apiCall` = `apiCallAttempt` = the widest
    * accessor deadline, with `socket` its own knob underneath.
    *
    * The two SDK-level values are derived rather than exposed, which is what makes an incoherent budget
    * unconfigurable. A transport ceiling is required, not hygiene: [[Deadlines]]'s `.disconnect` frees the caller
    * while the attempt runs on, and without one that orphan is a leaked blocking-pool thread per read.
    *
    * Why `apiCallAttempt` must equal `apiCall`, why shortening `apiCall` below the accessor deadline was tried and
    * reverted, and why `socket` cannot bound an upload: `docs/adr/0009-bound-every-body-store-operation.md` (#222).
    */
  private[ccas] final case class S3Timeouts(
    connect: Duration,
    socket: Duration,
    apiCallAttempt: Duration,
    apiCall: Duration
  )

  private[ccas] def s3Timeouts(config: BodyStoreConfig, limits: BodyStoreLimits): Either[String, S3Timeouts] = {
    val widest = if (limits.read.compareTo(limits.write) >= 0) { limits.read } else { limits.write }
    for {
      connect <- positiveMs(config.s3ConnectTimeoutMs, DefaultS3ConnectTimeoutMs, "body-store.s3-connect-timeout-ms")
      socket  <- positiveMs(config.s3SocketTimeoutMs, DefaultS3SocketTimeoutMs, "body-store.s3-socket-timeout-ms")
      _ <- Either.cond(
        socket.compareTo(widest) <= 0,
        (),
        s"body-store.s3-socket-timeout-ms (${socket.toMillis}) must not exceed the widest accessor deadline " +
          s"(${widest.toMillis}ms): the transport would outlive the operation it serves."
      )
    } yield S3Timeouts(connect = connect, socket = socket, apiCallAttempt = widest, apiCall = widest)
  }

  private def positiveMs(configured: Option[Int], default: Int, key: String): Either[String, Duration] =
    configured.getOrElse(default) match {
      case ms if ms > 0 => Right(Duration.fromMillis(ms.toLong))
      case ms           => Left(s"$key must be a positive number of milliseconds, got $ms")
    }

  /** Raw config mapping 1:1 to HOCON keys under `body-store`. `backend` selects the impl; `fsRoot` overrides the
    * local filesystem root for `fs` (absent = [[resolveFsRoot]]'s XDG default); the `s3*` fields are only required
    * when `backend = "s3"` and are validated at layer-build time. S3 fields are `Option` so the `fs` path (and CI,
    * which has no R2 creds) loads cleanly.
    *
    * The four `*-timeout-ms` keys are `Option` with compiled-in defaults rather than HOCON literals, mirroring
    * `s3Region`: a literal would have to be duplicated into `src/test/resources/application.conf`, where forgetting
    * it fails every suite that builds [[live]] rather than falling back.
    */
  @kebabCase
  private[ccas] final case class BodyStoreConfig(
    backend: String,
    fsRoot: Option[String],
    readTimeoutMs: Option[Int],
    writeTimeoutMs: Option[Int],
    s3Endpoint: Option[String],
    s3Bucket: Option[String],
    s3Region: Option[String],
    s3AccessKey: Option[String],
    s3SecretKey: Option[String],
    s3ConnectTimeoutMs: Option[Int],
    s3SocketTimeoutMs: Option[Int]
  )

  private[ccas] object BodyStoreConfig {
    given DeriveConfig[BodyStoreConfig] = DeriveConfig.derived
  }

  /** Reads the `body-store` config section and builds the selected backend, wrapped in [[Deadlines]] and then
    * [[HealthLogging]]. Scoped, because the S3 client is closable. Fails the layer with a clear message on an
    * unknown backend, an unresolvable `fs` root, missing S3 credentials, or an incoherent timeout budget.
    *
    * Decorator order is load-bearing and invisible at the callsite: `Deadlines` must nest INSIDE `HealthLogging`,
    * or a store slow enough to blow every deadline never flips the degraded flag. `TestBodyStore` pins both orders
    * — see `docs/adr/0009-bound-every-body-store-operation.md`.
    */
  val live: RLayer[Any, BodyStore] =
    ZLayer.scoped {
      val provider = TypesafeConfigProvider.fromTypesafeConfig(
        ConfigFactory.load(), enableCommaSeparatedValueAsList = true
      )
      for {
        config   <- provider.load(summon[DeriveConfig[BodyStoreConfig]].desc.nested("body-store"))
        limits   <- ZIO.fromEither(limitsFrom(config)).mapError(new IllegalArgumentException(_))
        store    <- build(config, limits)
        degraded <- Ref.make(false)
      } yield new HealthLogging(new Deadlines(store, limits), degraded)
    }

  private def build(config: BodyStoreConfig, limits: BodyStoreLimits): ZIO[Scope, Throwable, BodyStore] =
    config.backend match {
      case "fs" => buildFs(config)
      case "s3" => buildS3(config, limits)
      case other =>
        ZIO.fail(new IllegalArgumentException(s"Unknown body-store.backend '$other' (expected 'fs' or 's3')"))
    }

  private def buildFs(config: BodyStoreConfig): ZIO[Any, Throwable, BodyStore] =
    for {
      configured <- ZIO
        .fromEither(
          resolveFsRoot(
            configured = config.fsRoot,
            xdgCacheHome = Option(System.getenv("XDG_CACHE_HOME")),
            userHome = Option(System.getProperty("user.home"))
          )
        )
        .mapError(new IllegalArgumentException(_))
      // Absolutised once here rather than left relative: an explicitly-configured relative `fs-root` is honoured
      // (it was deliberate) but it resolves against whatever directory the process was launched from — the
      // CWD-dependence this issue removed from the *default*. Pin it at startup, log where it actually landed, and
      // say so, so nobody discovers it by finding blobs in two places.
      root = configured.toAbsolutePath
      _ <- ZIO.whenDiscard(!configured.isAbsolute)(
        ZIO.logWarning(s"body-store.fs-root '$configured' is relative; resolved against the launch directory to $root")
      )
      _ <- ZIO.logDebug(s"BodyStore: fs backend rooted at $root")
    } yield new FsBodyStore(root)

  /** Resolve the `fs` backend's root: an explicit, non-blank `body-store.fs-root` wins, else
    * `${XDG_CACHE_HOME:-$HOME/.cache}/ccas/bodies`.
    *
    * Resolved in code rather than as a HOCON literal, and every fallback blank-checked, so an unresolvable root
    * returns `Left` instead of a CWD-relative best guess. The XDG lookup is inlined rather than reusing
    * `ccas.cli.XdgPaths` because this package must not depend on `ccas.cli` — keep the two in step. Why each of
    * those: `docs/adr/0008-body-store-outside-postgres.md`.
    */
  private[ccas] def resolveFsRoot(
    configured: Option[String],
    xdgCacheHome: Option[String],
    userHome: Option[String]
  ): Either[String, Path] =
    nonBlank(configured) match {
      case Some(root) => Right(Paths.get(root))
      case None =>
        nonBlank(xdgCacheHome)
          .orElse(nonBlank(userHome).map(home => s"$home/.cache"))
          .toRight(
            "Cannot resolve body-store.fs-root: neither XDG_CACHE_HOME nor the user.home JVM property is set. " +
              "Set CCAS_BODY_STORE_FS_ROOT to an absolute path."
          )
          .map(base => Paths.get(base, "ccas", "bodies"))
    }

  private def nonBlank(value: Option[String]): Option[String] = value.map(_.trim).filter(_.nonEmpty)

  private def buildS3(config: BodyStoreConfig, limits: BodyStoreLimits): ZIO[Scope, Throwable, BodyStore] =
    for {
      endpoint  <- required(config.s3Endpoint, "body-store.s3-endpoint")
      bucket    <- required(config.s3Bucket, "body-store.s3-bucket")
      accessKey <- required(config.s3AccessKey, "body-store.s3-access-key")
      secretKey <- required(config.s3SecretKey, "body-store.s3-secret-key")
      timeouts  <- ZIO.fromEither(s3Timeouts(config, limits)).mapError(new IllegalArgumentException(_))
      region = config.s3Region.getOrElse("auto")
      client <- ZIO.acquireRelease(
        ZIO.attemptBlocking(buildS3Client(endpoint, region, accessKey, secretKey, timeouts))
      )(c => ZIO.attemptBlocking(c.close()).ignore)
      // Mirrors the fs backend's startup line so "where are my bodies going?" is answerable from the log either
      // way. Endpoint / bucket / region only — the access and secret keys never reach a log.
      _ <- ZIO.logDebug(s"BodyStore: s3 backend at $endpoint, bucket '$bucket', region '$region'")
    } yield new S3BodyStore(client, bucket)

  private[ccas] def buildS3Client(
    endpoint: String,
    region: String,
    accessKey: String,
    secretKey: String,
    timeouts: S3Timeouts
  ): S3Client =
    S3Client
      .builder()
      .endpointOverride(URI.create(endpoint))
      .region(Region.of(region))
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
      // R2 (and most S3-compatible endpoints) require path-style addressing rather than virtual-hosted buckets.
      .forcePathStyle(true)
      .overrideConfiguration(s3Overrides(timeouts))
      // httpClientBuilder (not httpClient): the SDK then owns the transport's lifecycle and closes it when the
      // S3Client is closed on scope exit — a passed-in instance would be left open.
      .httpClientBuilder(s3HttpBuilder(timeouts))
      .build()

  // Extracted so the mapping from S3Timeouts onto the SDK's two API-call budgets is assertable in isolation. The
  // wiring into `buildS3Client` — the defect being fixed here is literally "the builder was called bare" — is
  // pinned separately by reading `serviceClientConfiguration` back off a built client. The HTTP client's own
  // connect/socket values have no such getter, so `s3HttpBuilder` stays asserted on the pure builder input.
  private[ccas] def s3Overrides(timeouts: S3Timeouts): ClientOverrideConfiguration =
    ClientOverrideConfiguration
      .builder()
      .apiCallTimeout(timeouts.apiCall)
      .apiCallAttemptTimeout(timeouts.apiCallAttempt)
      .build()

  private[ccas] def s3HttpBuilder(timeouts: S3Timeouts): UrlConnectionHttpClient.Builder =
    UrlConnectionHttpClient
      .builder()
      .connectionTimeout(timeouts.connect)
      .socketTimeout(timeouts.socket)

  private def required(value: Option[String], key: String): ZIO[Any, Throwable, String] =
    ZIO
      .fromOption(nonBlank(value))
      .orElseFail(new IllegalArgumentException(s"body-store.backend='s3' requires $key to be set"))

  /** Backend decorator bounding every operation's wall time, so a merely slow store degrades like a broken one.
    *
    * `.disconnect` is mandatory: a plain `.timeout` waits for the inner effect's interruption, which a socket read
    * does not honour, so the caller hangs for exactly the duration being bounded. Policy-free by design — it raises
    * a typed failure and nothing else, leaving [[read]] and [[putOrSkip]] to fold it into a miss.
    *
    * `delete` rides the write budget deliberately: `Tables.ensureTables` drives it once per swept hash on every CLI
    * invocation, so an unbounded delete is a boot hang. Measurements and the transport ceiling:
    * `docs/adr/0009-bound-every-body-store-operation.md` (#211).
    */
  private[ccas] final class Deadlines(underlying: BodyStore, limits: BodyStoreLimits) extends BodyStore {

    def get(hash: String): Task[Option[Array[Byte]]] =
      bounded(op = "read", limit = limits.read, bytes = None, effect = underlying.get(hash))

    def put(hash: String, bytes: Array[Byte]): Task[Unit] =
      bounded(op = "write", limit = limits.write, bytes = Some(bytes.length), effect = underlying.put(hash, bytes))

    def delete(hash: String): Task[Unit] =
      bounded(op = "delete", limit = limits.write, bytes = None, effect = underlying.delete(hash))

    private def bounded[A](op: String, limit: Duration, bytes: Option[Int], effect: Task[A]): Task[A] =
      effect.disconnect.timeoutFail(new BodyStoreTimeoutException(op, limit, bytes))(limit)
  }

  /** Backend decorator tracking store health and logging only the transitions — one WARN when the store first
    * fails, one INFO when it next succeeds — instead of one line per failed operation.
    *
    * Errors are re-raised unchanged; this layer only observes. Why the split matters, why #199's budget guard
    * belongs above this decorator rather than below it, and why the transition log is a lower bound on failures
    * rather than a count: `docs/adr/0008-body-store-outside-postgres.md`.
    */
  private[ccas] final class HealthLogging(underlying: BodyStore, degraded: Ref[Boolean]) extends BodyStore {

    def get(hash: String): Task[Option[Array[Byte]]] = track("read", underlying.get(hash))

    def put(hash: String, bytes: Array[Byte]): Task[Unit] = track("write", underlying.put(hash, bytes))

    def delete(hash: String): Task[Unit] = track("delete", underlying.delete(hash))

    private def track[A](op: String, effect: Task[A]): Task[A] =
      effect.tapBoth(recordFailure(op, _), _ => recordSuccess)

    // getAndSet, so only the fiber that observes the flag still clear emits the WARN — concurrent failures during
    // one outage can't each announce it.
    private def recordFailure(op: String, error: Throwable): UIO[Unit] =
      ZIO.whenZIODiscard(degraded.getAndSet(true).negate)(
        ZIO.logWarning(
          s"BodyStore unavailable (first failure on $op): ${error.safeMessage}. Response-body caching is " +
            "degraded to no-cache until it recovers; requests continue, uncached."
        )
      )

    // Must stay above `recordSuccess`, which reads it: a `val` initialiser that reads a later `val` sees null.
    private val clearDegraded: UIO[Unit] =
      ZIO.whenZIODiscard(degraded.getAndSet(false))(ZIO.logInfo("BodyStore recovered; response-body caching resumed"))

    // Read first: the happy path is every successful read/write, so it must not write to the Ref.
    private val recordSuccess: UIO[Unit] =
      ZIO.whenZIODiscard(degraded.get)(clearDegraded)
  }
}
