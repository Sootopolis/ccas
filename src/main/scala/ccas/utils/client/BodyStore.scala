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

/** Content-addressed store for cached Chess.com response bodies, keyed by the SHA-256 hash already computed in
  * [[ccas.analysis.tables.ApiResponseBody]]. Keeping bodies here (rather than in a `body` column on metered
  * Postgres) is the fix for #191: the blob cache is not source of truth (a lost body just re-fetches from
  * Chess.com), so it has no business round-tripping through Neon egress on every cache hit.
  *
  * The key is the existing hex SHA-256, so `put` is naturally idempotent (byte-identical bodies collide on the same
  * key) and dedup is preserved for free. `get` returns `None` for a missing key, which the caller treats as a cache
  * miss and heals via a network refetch — the same mid-flight-race path that already existed when a body row was
  * pruned out from under a `Fresh` / `Revalidated` result.
  *
  * The trait itself is honest about I/O (`Task`); the '''degradation policy''' lives in the companion's accessors
  * ([[BodyStore.read]] / [[BodyStore.putOrSkip]]), which is what persistence code calls. Because the cache is
  * not source of truth, a store outage must degrade the cache to "off" rather than fail live requests (#200), and a
  * store *stall* must do the same — [[BodyStore.Deadlines]] bounds every operation so a slow store cannot become
  * our latency (#211).
  */
trait BodyStore {
  def get(hash: String): Task[Option[Array[Byte]]]
  def put(hash: String, bytes: Array[Byte]): Task[Unit]
  def delete(hash: String): Task[Unit]
}

/** Outcome of a [[BodyStore]] read once the store's own failures have been absorbed.
  *
  * The three cases exist because "the object is genuinely absent" and "the store could not answer" are the same
  * `None` at the trait level but demand '''different''' repairs upstream (#215). A `Missing` object means the
  * `api_response_cache` row pointing at it is a lie, so dropping that row is the fix. An `Unavailable` store means
  * the row is perfectly good — it still holds the ETag and `max-age` that make the next request cheap — so dropping
  * it converts a transient outage into a permanently cold cache, and every URL touched during the outage pays a full
  * unconditional GET after recovery instead of a 304.
  *
  * The backends already draw this line correctly (`NoSuchFileException` / `NoSuchKeyException` map to `None`, and
  * everything else stays a typed error); this type is what stops the accessor from throwing it away again.
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

  /** Read a body's bytes, absorbing a store error (or a [[Deadlines]] breach) into [[BodyRead.Unavailable]] while
    * keeping it distinct from the [[BodyRead.Missing]] a genuinely absent object produces.
    *
    * Both cases are healed by a network refetch, so an object-store outage (R2 down, credentials rotated, disk
    * unreadable, or merely too slow to answer) degrades the body cache to "no cache" instead of failing live
    * requests. They differ in what happens to the *metadata*: only `Missing` justifies dropping the cache row — see
    * [[BodyRead]]. Interruption still propagates (`catchAll` covers typed failures only), so shutdown is unaffected.
    *
    * Per-operation failures log at DEBUG on purpose: an outage fails *every* read on a fetch-heavy run, and one
    * WARN each would bury the log. The operator-facing signal is the single WARN [[HealthLogging]] emits when the
    * store first goes down (and the INFO when it comes back).
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
    * can never be served — every read of it falls through to a refetch, so it costs a Postgres row and buys nothing.
    * Skipping the whole cache write instead means an outage degrades to "no cache" and self-heals on the first
    * successful put. #199's budget guard reuses this exact skip (a `put` that no-ops without failing the fetch).
    *
    * DEBUG-level per-failure logging, for the same reason as [[read]].
    *
    * '''Known leak.''' A write abandoned at its [[Deadlines]] budget may still complete against the store
    * afterwards, while this returns `false` and the caller correctly declines to write the pointer row. The
    * resulting object has no `api_response_body` row, and [[ccas.analysis.tables.ApiResponseBody.deleteOrphanRows]]
    * enumerates pointer *rows*, so no sweep can ever see it. It self-heals only if the identical body is fetched
    * again (content-addressing means a later put adopts it). Accepted rather than reconciled — a sweep able to find
    * these would have to enumerate the bucket, which is what content-addressing exists to avoid.
    *
    * The same permanent leak already exists on the delete side: [[ccas.analysis.tables.ApiResponseBody.deleteOrphans]]
    * drops the pointer row FIRST and then deletes the object best-effort, so a delete that fails — or, since #211,
    * one that outruns its deadline — strands the object with the row that named it already gone. Bounding `delete`
    * therefore raises the rate of a pre-existing leak; it does not introduce one.
    *
    * Mitigation is an '''age-based''' bucket lifecycle rule, not a reference-aware one — S3/R2 rules cannot see our
    * pointer table. Set the expiry above the longest retention window (`cache_retention_days`,
    * `fetch_failure_retention_days`) and it collects only true orphans. Setting it too low is degrading rather than
    * corrupting, but the two kinds of object degrade differently: a cached response body expired early reads back as
    * [[BodyRead.Missing]], which invalidates the row and refetches (cost: one wasted request), whereas an
    * `api_fetch_failure` body has no refetch path at all — `ApiFetchFailure.selectRecent` simply renders the audit
    * row without its body, so the evidence is gone for good. Revisit the rule if either retention window is ever
    * raised past it. Tracked as an input to #199, since these are billed bytes no accounting in this codebase can
    * see.
    */
  def putOrSkip(hash: String, bytes: Array[Byte]): URIO[BodyStore, Boolean] =
    ZIO.serviceWithZIO[BodyStore](_.put(hash, bytes)).as(true).catchAll { e =>
      ZIO.logDebug(s"BodyStore write failed for $hash, body not cached: ${e.safeMessage}").as(false)
    }

  /** Delete an object. Left in raw `Task` form because every callsite is already a best-effort `.ignore` — orphan
    * cleanup failing just leaves a harmless content-addressed object behind.
    */
  def delete(hash: String): ZIO[BodyStore, Throwable, Unit] =
    ZIO.serviceWithZIO[BodyStore](_.delete(hash))

  /** Raised by [[Deadlines]] when an operation outruns its budget. A named type (rather than a bare
    * `TimeoutException`) so a later latency breaker can match on it; the message is what the accessors' existing
    * `safeMessage` DEBUG line prints.
    *
    * No stack trace: this is control flow on the hot read path during exactly the incident where throughput matters,
    * not a defect, and the frames would name the decorator rather than anything diagnostic.
    */
  final class BodyStoreTimeoutException(val op: String, val limit: Duration)
      extends RuntimeException(s"BodyStore $op exceeded its ${limit.toMillis}ms deadline") {
    override def fillInStackTrace(): Throwable = this
  }

  private[ccas] val DefaultReadTimeoutMs      = 5000
  private[ccas] val DefaultWriteTimeoutMs     = 10000
  private[ccas] val DefaultS3ConnectTimeoutMs = 2000
  private[ccas] val DefaultS3SocketTimeoutMs  = 5000

  /** Per-operation wall-clock budgets for the store, from `body-store.{read,write}-timeout-ms`.
    *
    * Reads get the tighter budget: abandoning one costs an immediate Chess.com request, whereas abandoning a write
    * costs only a future refetch that may never happen. Writes are nonetheless deliberately generous, because a
    * write abandoned mid-flight is what manufactures the uncollectable orphan objects documented on [[putOrSkip]].
    *
    * Both defaults are deliberately loose. Set below the store's true p99 and the body cache silently becomes an
    * '''amplifier''' of Chess.com load — each bypassed read costs an unconditional GET plus gate wait plus EMA
    * delay, spending the scarce resource to save the abundant one. Tighten from measurement (`cache_unserved` and
    * the store's own latency distribution), not from argument; see #211.
    */
  private[ccas] final case class BodyStoreLimits(read: Duration, write: Duration)

  private[ccas] def limitsFrom(config: BodyStoreConfig): Either[String, BodyStoreLimits] =
    for {
      read  <- positiveMs(config.readTimeoutMs, DefaultReadTimeoutMs, "body-store.read-timeout-ms")
      write <- positiveMs(config.writeTimeoutMs, DefaultWriteTimeoutMs, "body-store.write-timeout-ms")
    } yield BodyStoreLimits(read, write)

  /** The S3 transport's own budget, derived from the accessor deadlines: `apiCall` = the '''widest''' accessor
    * deadline >= `apiCallAttempt` = socket timeout.
    *
    * One `S3Client` carries one `apiCallTimeout`, so the ceiling is sized for the widest operation (`write`, by
    * default). A read abandoned at its own 5s deadline is therefore capped by the transport at the 10s write budget
    * rather than at 5s — the orphan is bounded, not made as short as the accessor that spawned it.
    *
    * This is not hygiene. `attemptBlockingInterrupt` cannot unblock a `UrlConnectionHttpClient` socket read —
    * `Thread.interrupt` does not reach it — so [[Deadlines]]'s `.disconnect` frees the *caller* at the deadline
    * while the attempt runs on. Without a transport ceiling that orphan is a leaked blocking-pool thread per read.
    * The SDK ships `DEFAULT_SOCKET_READ_TIMEOUT = 30s` / `DEFAULT_CONNECTION_TIMEOUT = 2s` and no API-call ceiling
    * at all, and [[buildS3Client]] used to set none of them.
    *
    * `apiCallAttempt` and `apiCall` are '''derived''' rather than exposed as knobs, which is what makes an
    * incoherent budget unconfigurable. Retry count stays at the SDK default: `apiCallTimeout` is an absolute ceiling
    * across all attempts, so the attempt count cannot extend it.
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
    } yield S3Timeouts(connect = connect, socket = socket, apiCallAttempt = socket, apiCall = widest)
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
    * [[HealthLogging]] so an outage is announced once rather than once per operation. Scoped because the S3 client is
    * a closable resource. Fails the layer with a clear message on an unknown backend, an unresolvable `fs` root,
    * missing S3 credentials, or an incoherent timeout budget.
    *
    * '''Decorator order is load-bearing and invisible at the callsite.''' `Deadlines` must nest INSIDE
    * `HealthLogging`: `HealthLogging.track` observes via `tapBoth`, which does not fire on interruption, and
    * `Deadlines` interrupts the inner effect. With the order inverted a store slow enough to blow every deadline
    * never flips the degraded flag, so the operator loses the one WARN that says the cache is off. Verified both
    * ways; `TestBodyStore` pins it, because nothing else would catch the inversion.
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

  /** Resolve the `fs` backend's root directory: an explicit, non-blank `body-store.fs-root` wins; otherwise
    * `${XDG_CACHE_HOME:-$HOME/.cache}/ccas/bodies`.
    *
    * The default is resolved here rather than as a HOCON literal because a relative literal (the old
    * `cache/bodies`) is CWD-dependent — it dropped runtime blobs wherever the process happened to be started,
    * typically the repo tree. The XDG lookup is inlined rather than delegating to `ccas.cli.XdgPaths.cacheDir`
    * because `ccas.utils.client` must not depend on `ccas.cli` (wrong layering); keep the two in step if the
    * convention changes.
    *
    * Every fallback is checked for blankness and the no-source case returns a `Left` rather than a best guess.
    * `Paths.get("")` is the current directory and `Paths.get(null + "/.cache")` is a *relative* directory literally
    * named "null" — either would silently reinstate the CWD-dependence this method exists to remove, so an
    * unresolvable root fails the layer with an actionable message instead.
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

  /** Backend decorator that bounds every operation's wall time, so a store that is merely '''slow''' degrades the
    * same way a broken one already did (#211). The cache is not source of truth, so waiting on it is never
    * obligatory; the previous code had no ceiling anywhere, and the S3 SDK's own defaults bound a socket read rather
    * than an operation.
    *
    * `.disconnect` is not optional. A plain `.timeout` waits for the inner effect's *interruption* to complete, and
    * neither a `UrlConnectionHttpClient` socket read nor a stalled network-mount `Files.readAllBytes` honours
    * `Thread.interrupt` — so without it the caller still hangs for exactly the duration being bounded (measured: a
    * 300ms budget on a 3000ms operation returned at 3020ms without, 312ms with). With it, the caller returns at the
    * deadline and the orphaned attempt unwinds in the background, itself bounded by the transport timeouts
    * [[S3Timeouts]] sets. An outer interruption still propagates, so shutdown is unaffected.
    *
    * Policy-free by design: this raises a typed failure and nothing else, so [[read]]'s and [[putOrSkip]]'s existing
    * `catchAll` folds a stall into the same [[BodyRead.Unavailable]] / `false` an error produces. The decorator
    * enforces the clock, [[HealthLogging]] observes, the accessors decide what failure means.
    *
    * `delete` rides the write budget. Not incidental: `Tables.ensureTables` drives `BodyStore.delete` once per swept
    * hash on '''every''' CLI invocation, so an unbounded delete is a boot hang on a command the user typed.
    */
  private[ccas] final class Deadlines(underlying: BodyStore, limits: BodyStoreLimits) extends BodyStore {

    def get(hash: String): Task[Option[Array[Byte]]] = bounded("read", limits.read, underlying.get(hash))

    def put(hash: String, bytes: Array[Byte]): Task[Unit] = bounded("write", limits.write, underlying.put(hash, bytes))

    def delete(hash: String): Task[Unit] = bounded("delete", limits.write, underlying.delete(hash))

    private def bounded[A](op: String, limit: Duration, effect: Task[A]): Task[A] =
      effect.disconnect.timeoutFail(new BodyStoreTimeoutException(op, limit))(limit)
  }

  /** Backend decorator that tracks store health and logs the '''transitions''' — one WARN when the store first
    * fails, one INFO when it next succeeds — instead of one line per failed operation.
    *
    * Without this, an R2 outage on a fetch-heavy run emits a WARN for every read and every write (thousands of
    * identical lines), which buries the signal it is trying to raise. Errors are re-raised unchanged: this layer
    * only observes. Turning them into cache misses stays the job of [[read]] / [[putOrSkip]], so health
    * reporting and degradation policy remain separable — the split #199's budget guard slots into.
    *
    * When that guard lands it belongs '''above''' this decorator, not below it next to [[Deadlines]]: a quota
    * decline is a deliberate policy decision, and observed from underneath it would flip the degraded flag and log
    * "BodyStore unavailable" on every skipped write.
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
