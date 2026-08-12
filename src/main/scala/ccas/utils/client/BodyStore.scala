package ccas.utils.client

import java.net.URI
import java.nio.file.{Path, Paths}

import com.typesafe.config.ConfigFactory
import zio.{Ref, RLayer, Scope, Task, UIO, URIO, ZIO, ZLayer}
import zio.config.derivation.kebabCase
import zio.config.magnolia.DeriveConfig
import zio.config.typesafe.TypesafeConfigProvider

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
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
  * ([[BodyStore.getOrMiss]] / [[BodyStore.putOrSkip]]), which is what persistence code calls. Because the cache is
  * not source of truth, a store outage must degrade the cache to "off" rather than fail live requests (#200).
  */
trait BodyStore {
  def get(hash: String): Task[Option[Array[Byte]]]
  def put(hash: String, bytes: Array[Byte]): Task[Unit]
  def delete(hash: String): Task[Unit]
}

object BodyStore {

  // Service accessors so table code can require `BodyStore` from the ZIO environment alongside `PostgresClient`,
  // provided once at each callsite via `ZEnvironment(pgClient, bodyStore)`. Reads and writes are deliberately
  // exposed ONLY in their error-degrading form — there is no raw `get`/`put` accessor to reach for by mistake.

  /** Read a body's bytes, collapsing both a missing object '''and a store error''' to `None`.
    *
    * `None` is the cache-miss signal every caller already heals with a network refetch, so an object-store outage
    * (R2 down, credentials rotated, disk unreadable) degrades the body cache to "no cache" instead of failing live
    * requests. Interruption still propagates (`catchAll` covers typed failures only), so shutdown is unaffected.
    *
    * Per-operation failures log at DEBUG on purpose: an outage fails *every* read on a fetch-heavy run, and one
    * WARN each would bury the log. The operator-facing signal is the single WARN [[HealthLogging]] emits when the
    * store first goes down (and the INFO when it comes back).
    */
  def getOrMiss(hash: String): URIO[BodyStore, Option[Array[Byte]]] =
    ZIO.serviceWithZIO[BodyStore](_.get(hash)).catchAll { e =>
      ZIO.logDebug(s"BodyStore read failed for $hash, treating as a cache miss: ${e.safeMessage}").as(None)
    }

  /** Store a body's bytes, returning `false` when the store rejected the write.
    *
    * Callers must NOT write a hash-pointer row on `false`: a pointer with no object behind it is a cache entry that
    * can never be served — every read of it falls through to a refetch, so it costs a Postgres row and buys nothing.
    * Skipping the whole cache write instead means an outage degrades to "no cache" and self-heals on the first
    * successful put. #199's budget guard reuses this exact skip (a `put` that no-ops without failing the fetch).
    *
    * DEBUG-level per-failure logging, for the same reason as [[getOrMiss]].
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

  /** Raw config mapping 1:1 to HOCON keys under `body-store`. `backend` selects the impl; `fsRoot` overrides the
    * local filesystem root for `fs` (absent = [[resolveFsRoot]]'s XDG default); the `s3*` fields are only required
    * when `backend = "s3"` and are validated at layer-build time. S3 fields are `Option` so the `fs` path (and CI,
    * which has no R2 creds) loads cleanly.
    */
  @kebabCase
  private[ccas] final case class BodyStoreConfig(
    backend: String,
    fsRoot: Option[String],
    s3Endpoint: Option[String],
    s3Bucket: Option[String],
    s3Region: Option[String],
    s3AccessKey: Option[String],
    s3SecretKey: Option[String]
  )

  private[ccas] object BodyStoreConfig {
    given DeriveConfig[BodyStoreConfig] = DeriveConfig.derived
  }

  /** Reads the `body-store` config section and builds the selected backend, wrapped in [[HealthLogging]] so an
    * outage is announced once rather than once per operation. Scoped because the S3 client is a closable resource.
    * Fails the layer with a clear message on an unknown backend, an unresolvable `fs` root, or missing S3
    * credentials.
    */
  val live: RLayer[Any, BodyStore] =
    ZLayer.scoped {
      val provider = TypesafeConfigProvider.fromTypesafeConfig(
        ConfigFactory.load(), enableCommaSeparatedValueAsList = true
      )
      for {
        config   <- provider.load(summon[DeriveConfig[BodyStoreConfig]].desc.nested("body-store"))
        store    <- build(config)
        degraded <- Ref.make(false)
      } yield new HealthLogging(store, degraded)
    }

  private def build(config: BodyStoreConfig): ZIO[Scope, Throwable, BodyStore] =
    config.backend match {
      case "fs" => buildFs(config)
      case "s3" => buildS3(config)
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
      _ <- ZIO.unlessDiscard(configured.isAbsolute)(
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

  private def buildS3(config: BodyStoreConfig): ZIO[Scope, Throwable, BodyStore] =
    for {
      endpoint  <- required(config.s3Endpoint, "body-store.s3-endpoint")
      bucket    <- required(config.s3Bucket, "body-store.s3-bucket")
      accessKey <- required(config.s3AccessKey, "body-store.s3-access-key")
      secretKey <- required(config.s3SecretKey, "body-store.s3-secret-key")
      region = config.s3Region.getOrElse("auto")
      client <- ZIO.acquireRelease(
        ZIO.attemptBlocking(buildS3Client(endpoint, region, accessKey, secretKey))
      )(c => ZIO.attemptBlocking(c.close()).ignore)
      // Mirrors the fs backend's startup line so "where are my bodies going?" is answerable from the log either
      // way. Endpoint / bucket / region only — the access and secret keys never reach a log.
      _ <- ZIO.logDebug(s"BodyStore: s3 backend at $endpoint, bucket '$bucket', region '$region'")
    } yield new S3BodyStore(client, bucket)

  private def buildS3Client(endpoint: String, region: String, accessKey: String, secretKey: String): S3Client =
    S3Client
      .builder()
      .endpointOverride(URI.create(endpoint))
      .region(Region.of(region))
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
      // R2 (and most S3-compatible endpoints) require path-style addressing rather than virtual-hosted buckets.
      .forcePathStyle(true)
      // httpClientBuilder (not httpClient): the SDK then owns the transport's lifecycle and closes it when the
      // S3Client is closed on scope exit — a passed-in instance would be left open.
      .httpClientBuilder(UrlConnectionHttpClient.builder())
      .build()

  private def required(value: Option[String], key: String): ZIO[Any, Throwable, String] =
    ZIO
      .fromOption(nonBlank(value))
      .orElseFail(new IllegalArgumentException(s"body-store.backend='s3' requires $key to be set"))

  /** Backend decorator that tracks store health and logs the '''transitions''' — one WARN when the store first
    * fails, one INFO when it next succeeds — instead of one line per failed operation.
    *
    * Without this, an R2 outage on a fetch-heavy run emits a WARN for every read and every write (thousands of
    * identical lines), which buries the signal it is trying to raise. Errors are re-raised unchanged: this layer
    * only observes. Turning them into cache misses stays the job of [[getOrMiss]] / [[putOrSkip]], so health
    * reporting and degradation policy remain separable — the split #199's budget guard slots into.
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
      degraded.getAndSet(true).flatMap { wasDegraded =>
        ZIO.unlessDiscard(wasDegraded)(
          ZIO.logWarning(
            s"BodyStore unavailable (first failure on $op): ${error.safeMessage}. Response-body caching is " +
              "degraded to no-cache until it recovers; requests continue, uncached."
          )
        )
      }

    // Must stay above `recordSuccess`, which reads it: a `val` initialiser that reads a later `val` sees null.
    private val clearDegraded: UIO[Unit] =
      degraded.getAndSet(false).flatMap { stillDegraded =>
        ZIO.whenDiscard(stillDegraded)(ZIO.logInfo("BodyStore recovered; response-body caching resumed"))
      }

    // Read first: the happy path is every successful read/write, so it must not write to the Ref.
    private val recordSuccess: UIO[Unit] =
      degraded.get.flatMap(wasDegraded => ZIO.whenDiscard(wasDegraded)(clearDegraded))
  }
}
