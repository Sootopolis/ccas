package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.subtypes.{ApiResponseBodyId, ApiResponseCacheId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, withTransaction}

/** Persistent cache of Chess.com API responses keyed by URL. Holds cache-control metadata (ETag, Last-Modified,
  * max-age) alongside a foreign key into `api_response_body` for the body itself — body storage is deduplicated
  * by SHA-256 hash via `ApiResponseBody.ensureBody`, so repeated identical responses across different URLs share
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
    }

  /** Read cache metadata for a URL. Does not join to `api_response_body`; callers load the body only on demand. */
  def lookupMeta(url: String): ZIO[PostgresClient, SQLException, Option[ApiResponseCache]] =
    connectZIO {
      sql"""SELECT cache_id, fetched_at, url, content_type, max_age_seconds, etag, last_modified, body_id
            FROM api_response_cache WHERE url = $url""".query[ApiResponseCache].run().headOption
    }

  /** Upsert a fresh cache entry. Stores the body via `ApiResponseBody.ensureBody` (SHA-256 dedupe) and returns the
    * resolved `body_id` so the caller can compare against the prior `body_id` to detect byte-identical content.
    */
  def upsertWithBody(
    url: String,
    body: String,
    etag: Option[String],
    lastModified: Option[Instant],
    maxAgeSeconds: Option[Long],
    contentType: Option[String],
    fetchedAt: Instant
  ): ZIO[PostgresClient, SQLException, ApiResponseBodyId] =
    withTransaction {
      ApiResponseBody.ensureBody(body).flatMap { bodyId =>
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

  /** 304 Not Modified refresh: bumps `fetched_at` without touching the body or validators. */
  def touch(url: String, fetchedAt: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"UPDATE api_response_cache SET fetched_at = $fetchedAt WHERE url = $url".update.run()
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
  def deleteBefore(cutoff: Instant): ZIO[PostgresClient, SQLException, Int] =
    withTransaction {
      connectZIO {
        sql"DELETE FROM api_response_cache WHERE fetched_at < $cutoff".update.run()
      }.flatMap { count =>
        ApiResponseBody.deleteOrphans.as(count)
      }
    }
}
