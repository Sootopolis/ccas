package ccas.utils.client

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  DeleteObjectRequest,
  GetObjectRequest,
  NoSuchKeyException,
  PutObjectRequest
}
import zio.{Task, ZIO}

/** S3-API [[BodyStore]] pointed at Cloudflare R2 via an endpoint override (also covers B2 / MinIO / S3). The object
  * key is `bodies/<hash>`, so `put` is idempotent, and every object sits under one prefix that the bucket's lifecycle
  * rule is scoped to (#242): the layout is `docs/adr/0008-body-store-outside-postgres.md`, the rule and its margin
  * `docs/adr/0009-bound-every-body-store-operation.md`. Uses the AWS SDK v2 synchronous client wrapped in
  * `attemptBlockingInterrupt`, matching the codebase's JDBC idiom, rather than the netty async client — the cache
  * always materialises the full body for a whole-String JSON decode, so streaming buys nothing at this size.
  *
  * R2 was chosen for zero egress on every read (the exact property a re-read-heavy cache needs); see #191.
  */
final class S3BodyStore(client: S3Client, bucket: String) extends BodyStore {

  // Fixed rather than configurable: an object written under any other prefix escapes the lifecycle rule.
  private def keyFor(hash: String): String = s"bodies/$hash"

  def get(hash: String): Task[Option[Array[Byte]]] =
    ZIO
      .attemptBlockingInterrupt {
        val request = GetObjectRequest.builder().bucket(bucket).key(keyFor(hash)).build()
        client.getObjectAsBytes(request).asByteArray()
      }
      .map(Option(_))
      .catchSome { case _: NoSuchKeyException => ZIO.none }

  def put(hash: String, bytes: Array[Byte]): Task[Unit] =
    ZIO.attemptBlockingInterrupt {
      val request = PutObjectRequest.builder().bucket(bucket).key(keyFor(hash)).build()
      client.putObject(request, RequestBody.fromBytes(bytes))
    }.unit

  // Idempotent: S3 DeleteObject returns success even when the key is already absent.
  def delete(hash: String): Task[Unit] =
    ZIO.attemptBlockingInterrupt {
      val request = DeleteObjectRequest.builder().bucket(bucket).key(keyFor(hash)).build()
      client.deleteObject(request)
    }.unit
}
