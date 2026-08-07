package ccas.utils.client

import java.net.URI
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import zio.{RLayer, Scope, Task, ZIO, ZLayer}
import zio.config.derivation.kebabCase
import zio.config.magnolia.DeriveConfig
import zio.config.typesafe.TypesafeConfigProvider

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

/** Content-addressed store for cached Chess.com response bodies, keyed by the SHA-256 hash already computed in
  * [[ccas.analysis.tables.ApiResponseBody]]. Keeping bodies here (rather than in a `body` column on metered
  * Postgres) is the fix for #191: the blob cache is not source of truth (a lost body just re-fetches from
  * Chess.com), so it has no business round-tripping through Neon egress on every cache hit.
  *
  * The key is the existing hex SHA-256, so `put` is naturally idempotent (byte-identical bodies collide on the same
  * key) and dedup is preserved for free. `get` returns `None` for a missing key, which the caller treats as a cache
  * miss and heals via a network refetch — the same mid-flight-race path that already existed when a body row was
  * pruned out from under a `Fresh` / `Revalidated` result.
  */
trait BodyStore {
  def get(hash: String): Task[Option[Array[Byte]]]
  def put(hash: String, bytes: Array[Byte]): Task[Unit]
  def delete(hash: String): Task[Unit]
}

object BodyStore {

  // Service accessors so table code can require `BodyStore` from the ZIO environment alongside `PostgresClient`,
  // provided once at each callsite via `ZEnvironment(pgClient, bodyStore)`.
  def get(hash: String): ZIO[BodyStore, Throwable, Option[Array[Byte]]] =
    ZIO.serviceWithZIO[BodyStore](_.get(hash))

  def put(hash: String, bytes: Array[Byte]): ZIO[BodyStore, Throwable, Unit] =
    ZIO.serviceWithZIO[BodyStore](_.put(hash, bytes))

  def delete(hash: String): ZIO[BodyStore, Throwable, Unit] =
    ZIO.serviceWithZIO[BodyStore](_.delete(hash))

  /** Raw config mapping 1:1 to HOCON keys under `body-store`. `backend` selects the impl; `fsRoot` is the local
    * filesystem root for `fs`; the `s3*` fields are only required when `backend = "s3"` and are validated at
    * layer-build time. S3 fields are `Option` so the `fs` path (and CI, which has no R2 creds) loads cleanly.
    */
  @kebabCase
  private[ccas] final case class BodyStoreConfig(
    backend: String,
    fsRoot: String,
    s3Endpoint: Option[String],
    s3Bucket: Option[String],
    s3Region: Option[String],
    s3AccessKey: Option[String],
    s3SecretKey: Option[String]
  )

  private[ccas] object BodyStoreConfig {
    given DeriveConfig[BodyStoreConfig] = DeriveConfig.derived
  }

  /** Reads the `body-store` config section and builds the selected backend. Scoped because the S3 client is a
    * closable resource. Fails the layer with a clear message on an unknown backend or missing S3 credentials.
    */
  val live: RLayer[Any, BodyStore] =
    ZLayer.scoped {
      val provider = TypesafeConfigProvider.fromTypesafeConfig(
        ConfigFactory.load(), enableCommaSeparatedValueAsList = true
      )
      for {
        config <- provider.load(summon[DeriveConfig[BodyStoreConfig]].desc.nested("body-store"))
        store  <- build(config)
      } yield store
    }

  private def build(config: BodyStoreConfig): ZIO[Scope, Throwable, BodyStore] =
    config.backend match {
      case "fs" => ZIO.succeed(new FsBodyStore(Paths.get(config.fsRoot)))
      case "s3" => buildS3(config)
      case other =>
        ZIO.fail(new IllegalArgumentException(s"Unknown body-store.backend '$other' (expected 'fs' or 's3')"))
    }

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
      .fromOption(value.filter(_.trim.nonEmpty))
      .orElseFail(new IllegalArgumentException(s"body-store.backend='s3' requires $key to be set"))
}
