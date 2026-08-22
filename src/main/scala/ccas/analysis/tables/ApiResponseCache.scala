package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.subtypes.{ApiResponseBodyId, ApiResponseCacheId}
import ccas.utils.client.BodyStore
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, withTransaction}

/** Persistent cache of Chess.com API responses keyed by URL. Holds cache-control metadata (ETag, Last-Modified,
  * max-age) alongside a foreign key into `api_response_body` for the body itself — body storage is deduplicated
  * by SHA-256 hash via `ApiResponseBody.putBody`, so repeated identical responses across different URLs share
  * a single row in the body table.
  *
  * Lookups via `lookupMeta` intentionally do not load the body — `ChessComClient.getCacheable` reads only this
  * row during cache-hit dispatch and defers the body fetch to `ApiResponseBody.loadById` inside a lazy `getValue`
  * Task. Callers that branch on `isUnchanged` without needing the value thus never touch body storage.
  */
final case class ApiResponseCache(
  cacheId: ApiResponseCacheId,
  fetchedAt: Instant,
  url: String,
  contentType: Option[String],
  maxAgeSeconds: Option[Long],
  etag: Option[String],
  lastModified: Option[Instant],
  bodyId: ApiResponseBodyId
) derives DbCodec

object ApiResponseCache {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS api_response_cache (
              cache_id        BIGSERIAL PRIMARY KEY,
              fetched_at      TIMESTAMPTZ NOT NULL,
              url             TEXT NOT NULL UNIQUE,
              content_type    TEXT,
              max_age_seconds BIGINT,
              etag            TEXT,
              last_modified   TIMESTAMPTZ,
              body_id         BIGINT NOT NULL REFERENCES api_response_body (body_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_api_response_cache_fetched_at
            ON api_response_cache (fetched_at)""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_api_response_cache_body_id
            ON api_response_cache (body_id)""".update.run()
    }

  /** Read cache metadata for a URL. Does not join to `api_response_body`; callers load the body only on demand. */
  def lookupMeta(url: String): ZIO[PostgresClient, SQLException, Option[ApiResponseCache]] =
    connectZIO {
      sql"""SELECT cache_id, fetched_at, url, content_type, max_age_seconds, etag, last_modified, body_id
            FROM api_response_cache WHERE url = $url""".query[ApiResponseCache].run().headOption
    }

  /** Upsert a fresh cache entry. Stores the body via `ApiResponseBody.putBody` (SHA-256 dedupe) — done BEFORE the
    * transaction opens so the object-store round-trip never holds a pooled connection idle-in-transaction — then
    * writes the hash-pointer and cache rows atomically, returning the resolved `body_id` so the caller can compare
    * against the prior `body_id` to detect byte-identical content.
    *
    * Returns `None` when the body store rejected the write: nothing is persisted at all (no pointer row, no cache
    * row), because a cache row whose body can't be read is a row that can only ever produce a refetch. The caller
    * still has the body in memory, so the request succeeds — uncached. See [[ccas.utils.client.BodyStore.putOrSkip]].
    */
  def upsertWithBody(
    url: String,
    body: String,
    etag: Option[String],
    lastModified: Option[Instant],
    maxAgeSeconds: Option[Long],
    contentType: Option[String],
    fetchedAt: Instant
  ): ZIO[PostgresClient & BodyStore, SQLException, Option[ApiResponseBodyId]] =
    ApiResponseBody.putBody(source = url, body = body).flatMap {
      case None => ZIO.none
      case Some(hash) =>
        upsertRow(
          hash = hash,
          url = url,
          etag = etag,
          lastModified = lastModified,
          maxAgeSeconds = maxAgeSeconds,
          contentType = contentType,
          fetchedAt = fetchedAt
        ).asSome
    }

  /** Write the hash-pointer row and the cache row for an already-stored body, atomically. Pure DB — no object-store
    * I/O inside the transaction.
    */
  private def upsertRow(
    hash: String,
    url: String,
    etag: Option[String],
    lastModified: Option[Instant],
    maxAgeSeconds: Option[Long],
    contentType: Option[String],
    fetchedAt: Instant
  ): ZIO[PostgresClient, SQLException, ApiResponseBodyId] =
    withTransaction {
      ApiResponseBody.ensureBodyPointer(hash).flatMap { bodyId =>
        val rawBodyId = ApiResponseBodyId.unwrap(bodyId)
        connectZIO {
          sql"""INSERT INTO api_response_cache
                  (fetched_at, url, content_type, max_age_seconds, etag, last_modified, body_id)
                VALUES ($fetchedAt, $url, $contentType, $maxAgeSeconds, $etag, $lastModified, $rawBodyId)
                ON CONFLICT (url) DO UPDATE SET
                  fetched_at      = EXCLUDED.fetched_at,
                  content_type    = EXCLUDED.content_type,
                  max_age_seconds = EXCLUDED.max_age_seconds,
                  etag            = EXCLUDED.etag,
                  last_modified   = EXCLUDED.last_modified,
                  body_id         = EXCLUDED.body_id""".update.run()
        }.as(bodyId)
      }
    }

  /** 304 Not Modified refresh: bumps `fetched_at` and merges any fresh validators / cache-control values from the
    * 304 response. `etag`, `lastModified`, and `contentType` use COALESCE semantics — a `Some` overwrites, a `None`
    * preserves the stored value — so a 304 that omits those headers (RFC 7232 §4.1 only requires ETag among them)
    * leaves the entry untouched. `maxAgeUpdate` is a [[MaxAgeUpdate]] tri-state covering the three wire-level
    * distinctions (no `Cache-Control` header / `no-cache` / `max-age=n`).
    */
  def touch(
    url: String,
    fetchedAt: Instant,
    etag: Option[String],
    lastModified: Option[Instant],
    maxAgeUpdate: MaxAgeUpdate,
    contentType: Option[String]
  ): ZIO[PostgresClient, SQLException, Int] = {
    def writeMaxAge(newMaxAge: Option[Long]) = connectZIO {
      sql"""UPDATE api_response_cache SET
              fetched_at      = $fetchedAt,
              etag            = COALESCE($etag, etag),
              last_modified   = COALESCE($lastModified, last_modified),
              max_age_seconds = $newMaxAge,
              content_type    = COALESCE($contentType, content_type)
            WHERE url = $url""".update.run()
    }
    maxAgeUpdate match {
      case MaxAgeUpdate.Preserve =>
        connectZIO {
          sql"""UPDATE api_response_cache SET
                  fetched_at    = $fetchedAt,
                  etag          = COALESCE($etag, etag),
                  last_modified = COALESCE($lastModified, last_modified),
                  content_type  = COALESCE($contentType, content_type)
                WHERE url = $url""".update.run()
        }
      case MaxAgeUpdate.Clear           => writeMaxAge(None)
      case MaxAgeUpdate.Overwrite(secs) => writeMaxAge(Some(secs))
    }
  }

  /** Three-state directive for `max_age_seconds` on a 304 touch. Encodes the wire-level distinction between
    * (1) no `Cache-Control` header at all, (2) `Cache-Control: no-cache`, and (3) `Cache-Control: max-age=n`.
    */
  enum MaxAgeUpdate {
    case Preserve
    case Clear
    case Overwrite(seconds: Long)
  }

  /** Drop a cache entry. Used on JSON decode failures (schema drift) so the next request refetches over the wire. */
  def invalidate(url: String): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM api_response_cache WHERE url = $url".update.run()
    }

  /** Retention cleanup: delete cache rows older than `cutoff`, then drop any `api_response_body` rows no longer
    * referenced by the cache or by `api_fetch_failure`. Wrapped in a single transaction so an unreferenced body
    * can't be observed between the two steps. Returns the number of cache rows deleted. Safe to call from app
    * startup; concurrent invocations are idempotent — PG MVCC lets each one see its own snapshot and later runs
    * either find no rows to delete or see overlapping sets, so the end state is identical.
    *
    * Readers mid-way through `loadAndDecode` when a prune runs are tolerated: `ApiResponseBody.loadById` returning
    * `None` (the body was pruned out from under a `Fresh` / `Revalidated` result) falls through to a fresh network
    * fetch via `ChessComClient.loadAndDecode`'s recovery path.
    */
  def deleteBefore(cutoff: Instant): ZIO[PostgresClient & BodyStore, SQLException, Int] =
    withTransaction {
      for {
        count  <- connectZIO(sql"DELETE FROM api_response_cache WHERE fetched_at < $cutoff".update.run())
        hashes <- ApiResponseBody.deleteOrphanRows
      } yield (count, hashes)
    }.flatMap { case (count, hashes) =>
      ZIO.foreachDiscard(hashes)(hash => BodyStore.delete(hash).ignore).as(count)
    }
}
