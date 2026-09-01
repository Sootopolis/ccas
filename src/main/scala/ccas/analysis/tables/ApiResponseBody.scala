package ccas.analysis.tables

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.{URIO, ZIO}

import ccas.analysis.tables.subtypes.ApiResponseBodyId
import ccas.utils.client.{BodyRead, BodyStore}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, withTransaction}

/** Content-addressed pointer index for cached Chess.com response bodies. Since #191 the bodies themselves live in a
  * [[BodyStore]] (Cloudflare R2 in prod, local filesystem in dev/test), keyed by the SHA-256 hash — the blob cache is
  * not source of truth, so it has no business round-tripping through metered Postgres egress. This table keeps only
  * `(body_id, body_hash)`: the surrogate `body_id` preserves the existing FKs from `api_response_cache` and
  * `api_fetch_failure`, and `body_hash` is both the dedup key and the BodyStore object key.
  */
final case class ApiResponseBody(
  bodyHash: String
) derives DbCodec

object ApiResponseBody {

  /** Canonical body stored for Cloudflare challenge 403 responses. Contains the CF challenge marker so that downstream
    * retry schedules (which pattern-match on the marker) continue to work.
    */
  val CfCanonicalBody = "[Cloudflare challenge: /cdn-cgi/challenge-platform/]"

  /** [[CfCanonicalBody]]'s SHA-256 — its `body_hash` in this table and its object key in the [[BodyStore]]. Exposed
    * because it is the one hash in the schema that is a compile-time constant rather than derived from a response.
    */
  val CfCanonicalHash: String = sha256(CfCanonicalBody)

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS api_response_body (
              body_id   BIGSERIAL PRIMARY KEY,
              body_hash TEXT NOT NULL UNIQUE
            )""".update.run()
    }

  /** Store a body's bytes in the [[BodyStore]] and return its SHA-256 hash — the store key and dedup key — or
    * `None` when the store rejected the write.
    *
    * Call this BEFORE opening the JDBC transaction that writes the pointer, cache or failure row; why, and why
    * put-then-insert is the safe order: `docs/adr/0008-body-store-outside-postgres.md`. `None` is the store-outage
    * signal (see [[BodyStore.putOrSkip]]): skip the pointer row entirely rather than failing the request.
    *
    * '''Pass both arguments by name.''' They are both `String`, so a transposition compiles and stores the URL as
    * the body, and every later read of that entry decodes a URL as JSON.
    */
  def putBody(source: String, body: String): URIO[BodyStore, Option[String]] = {
    val hash = sha256(body)
    BodyStore.putOrSkip(hash, body.getBytes(StandardCharsets.UTF_8), source).map(Option.when(_)(hash))
  }

  /** Upsert the hash-pointer row for an already-stored body (see [[putBody]]) and return its `body_id`. Pure DB, so
    * it composes inside the caller's `withTransaction` without holding a connection across any object-store I/O.
    * `DO UPDATE` (a no-op write of body_hash to itself) is required because `ON CONFLICT DO NOTHING ... RETURNING`
    * returns no row on conflict — we need a `body_id` in every case.
    */
  def ensureBodyPointer(hash: String): ZIO[PostgresClient, SQLException, ApiResponseBodyId] =
    connectZIO {
      sql"""INSERT INTO api_response_body (body_hash) VALUES ($hash)
            ON CONFLICT (body_hash) DO UPDATE SET body_hash = EXCLUDED.body_hash
            RETURNING body_id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))
      .map(ApiResponseBodyId.wrap)

  /** Read a cached body by id: resolve the hash-pointer row (a tiny Neon read) then load the bytes from the
    * [[BodyStore]]. Used by `CacheableResult.Fresh` / `Revalidated` lazy-loading. Every non-`Found` outcome is a
    * cache miss the caller heals with a network refetch; the two of them differ only in what happens to the
    * *metadata* row.
    *
    *   - [[BodyRead.Missing]] — the pointer row is gone (orphan cleanup) or the object is absent from the store. The
    *     `api_response_cache` row pointing here is a lie, so the caller drops it.
    *   - [[BodyRead.Unavailable]] — the store errored or outran its deadline. The cache row is still accurate and
    *     still holds the validators that make the next request cheap, so the caller '''keeps''' it and refetches
    *     unconditionally. Dropping it would convert a transient outage into a permanently cold cache (#215).
    *
    * Either way the refetch is unconditional rather than validated: a conditional GET could come back 304, leaving
    * us with metadata and still no body.
    */
  def loadById(bodyId: ApiResponseBodyId): ZIO[PostgresClient & BodyStore, SQLException, BodyRead[String]] =
    connectZIO {
      val raw = ApiResponseBodyId.unwrap(bodyId)
      sql"SELECT body_hash FROM api_response_body WHERE body_id = $raw".query[String].run().headOption
    }.flatMap {
      case Some(hash) => BodyStore.read(hash).map(_.map(bytes => new String(bytes, StandardCharsets.UTF_8)))
      case None       => ZIO.succeed(BodyRead.Missing)
    }

  /** Delete pointer rows no longer referenced by the cache or by `api_fetch_failure`, returning their freed hashes.
    * Pure DB (no object-store I/O), so callers can run it inside a `withTransaction` and delete the objects AFTER
    * commit — see [[deleteOrphans]] and the `deleteBefore` sweeps.
    */
  def deleteOrphanRows: ZIO[PostgresClient, SQLException, List[String]] =
    connectZIO {
      sql"""DELETE FROM api_response_body b
            WHERE NOT EXISTS (
              SELECT 1 FROM api_fetch_failure f WHERE f.response_body_id = b.body_id
            )
              AND NOT EXISTS (
              SELECT 1 FROM api_response_cache c WHERE c.body_id = b.body_id
            )
            RETURNING b.body_hash""".query[String].run().toList
    }

  /** Delete orphan pointer rows and then their [[BodyStore]] objects. Not for use inside a `withTransaction` (the
    * object deletes are network I/O — the `deleteBefore` sweeps use [[deleteOrphanRows]] + a post-commit object
    * delete instead). The object deletes are best-effort (`.ignore`): a failed delete leaves a harmless orphan
    * object (content-addressed, ~zero cost) rather than failing the DB cleanup. That leak is permanent — once the
    * pointer row is gone the hash is never revisited — but inconsequential for a cache, so it is accepted, not
    * reconciled.
    */
  def deleteOrphans: ZIO[PostgresClient & BodyStore, SQLException, Int] =
    deleteOrphanRows.flatMap(hashes => deleteFreedObjects(hashes).as(hashes.size))

  /** Run a caller's row delete and the orphan-pointer sweep in ONE transaction, then delete the freed objects outside
    * it. The split is the whole point and is easy to lose when open-coded: object deletes are network I/O, so running
    * them inside would hold a pooled connection idle-in-transaction for the length of the sweep. Returns the caller's
    * own row count. Shared by the retention sweeps in [[ApiResponseCache]] and [[ApiFetchFailure]].
    */
  private[tables] def deleteRowsAndSweepOrphans(
    deleteRows: ZIO[PostgresClient, SQLException, Int]
  ): ZIO[PostgresClient & BodyStore, SQLException, Int] =
    for {
      (count, hashes) <- withTransaction {
                           for {
                             count  <- deleteRows
                             hashes <- deleteOrphanRows
                           } yield (count, hashes)
                         }
      _ <- deleteFreedObjects(hashes)
    } yield count

  /** Delete the objects behind already-committed freed hashes. Bounded-parallel: a retention sweep can free tens of
    * thousands of hashes and each delete is a round trip, so sequential was minutes of wall clock.
    */
  private[tables] def deleteFreedObjects(hashes: List[String]): URIO[BodyStore, Unit] =
    ZIO.foreachParDiscard(hashes)(hash => BodyStore.delete(hash).ignore).withParallelism(ObjectDeleteParallelism)

  /** Deliberately modest: the sweep now runs against the same store the fetch path reads from, so a wide delete burst
    * buys sweep throughput with cache-read latency for its whole duration. Raise it from measurement
    * (`client_stats.cache_unserved` during a sweep), not from argument.
    */
  private[tables] val ObjectDeleteParallelism = 8

  /** Ensure the canonical Cloudflare-challenge body exists (object + pointer row). Idempotent — safe on every
    * startup. Going forward every CF 403 is written with [[CfCanonicalBody]] at fetch time (see
    * `ChessComClient.handleResponse`), so all CF failure rows already dedup to this one hash; any legacy per-request
    * CF variant rows from before that behaviour age out via the `api_fetch_failure` retention sweep.
    *
    * A store outage skips the pointer row and returns 0 rather than failing startup — this is only a pre-warm; the
    * first CF 403 recreates object + pointer through the normal [[putBody]] path.
    */
  def normalizeCfBodies: ZIO[PostgresClient & BodyStore, SQLException, Int] =
    // Uses the precomputed [[CfCanonicalHash]] rather than routing through [[putBody]], which would re-derive the
    // same constant SHA-256 on every startup. The put-before-pointer ordering putBody documents still holds.
    BodyStore
      .putOrSkip(CfCanonicalHash, CfCanonicalBody.getBytes(StandardCharsets.UTF_8), "startup Cloudflare pre-warm")
      .flatMap { stored =>
        if (stored) { insertPointerIfAbsent(CfCanonicalHash) }
        // Debug, not warn: `BodyStore`'s health decorator already raised one WARN for the outage itself, and this
        // adds only which pre-warm was skipped.
        else { ZIO.logDebug("BodyStore unavailable; skipping Cloudflare canonical-body pre-warm").as(0) }
      }

  private def insertPointerIfAbsent(hash: String): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO api_response_body (body_hash) VALUES ($hash)
            ON CONFLICT (body_hash) DO NOTHING""".update.run()
    }

  private def sha256(input: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes  = digest.digest(input.getBytes(StandardCharsets.UTF_8))
    bytes.map(b => String.format("%02x", b)).mkString
  }
}
