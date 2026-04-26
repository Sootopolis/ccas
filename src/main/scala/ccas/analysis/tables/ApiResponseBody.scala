package ccas.analysis.tables

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.subtypes.ApiResponseBodyId
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

final case class ApiResponseBody(
  bodyHash: String,
  body: String
) derives DbCodec

object ApiResponseBody {

  /** Canonical body stored for Cloudflare challenge 403 responses. Contains the CF challenge marker so that downstream
    * retry schedules (which pattern-match on the marker) continue to work.
    */
  val CfCanonicalBody = "[Cloudflare challenge: /cdn-cgi/challenge-platform/]"

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS api_response_body (
              body_id   BIGSERIAL PRIMARY KEY,
              body_hash TEXT NOT NULL UNIQUE,
              body      TEXT NOT NULL
            )""".update.run()
    }

  // Single-statement upsert: `DO UPDATE` (no-op write of body_hash to itself) is required because
  // `ON CONFLICT DO NOTHING ... RETURNING` returns no row on conflict — we need a row in every case.
  // This eliminates the SELECT/INSERT/SELECT race where `deleteOrphans` could prune the row in the gap.
  def ensureBody(body: String): ZIO[PostgresClient, SQLException, ApiResponseBodyId] = {
    val hash = sha256(body)
    connectZIO {
      sql"""INSERT INTO api_response_body (body_hash, body) VALUES ($hash, $body)
            ON CONFLICT (body_hash) DO UPDATE SET body_hash = EXCLUDED.body_hash
            RETURNING body_id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))
      .map(ApiResponseBodyId.wrap)
  }

  /** Read a cached body by id. Used by `CacheableResult.Fresh` / `Revalidated` lazy-loading to fetch the body only
    * when the caller actually invokes `getValue`. Returns `None` if the row was deleted (e.g. by orphan cleanup),
    * which the caller should treat as a cache miss and fall through to a network refetch.
    */
  def loadById(bodyId: ApiResponseBodyId): ZIO[PostgresClient, SQLException, Option[String]] =
    connectZIO {
      val raw = ApiResponseBodyId.unwrap(bodyId)
      sql"SELECT body FROM api_response_body WHERE body_id = $raw".query[String].run().headOption
    }

  def deleteOrphans: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""DELETE FROM api_response_body b
            WHERE NOT EXISTS (
              SELECT 1 FROM api_fetch_failure f WHERE f.response_body_id = b.body_id
            )
              AND NOT EXISTS (
              SELECT 1 FROM api_response_cache c WHERE c.body_id = b.body_id
            )""".update.run()
    }

  /** Normalize all Cloudflare challenge bodies to a single canonical row. Idempotent — safe on every startup. */
  def normalizeCfBodies: ZIO[PostgresClient, SQLException, Int] = {
    val canonicalHash = sha256(CfCanonicalBody)
    transactZIO {
      sql"""INSERT INTO api_response_body (body_hash, body) VALUES ($canonicalHash, $CfCanonicalBody)
            ON CONFLICT (body_hash) DO NOTHING""".update.run()
      val canonicalId = sql"SELECT body_id FROM api_response_body WHERE body_hash = $canonicalHash"
        .query[Long].run().head
      val updated = sql"""UPDATE api_fetch_failure SET response_body_id = $canonicalId
            WHERE response_body_id IN (
              SELECT body_id FROM api_response_body
              WHERE body LIKE '%/cdn-cgi/challenge-platform/%' AND body_id != $canonicalId
            )""".update.run()
      sql"""DELETE FROM api_response_body
            WHERE body LIKE '%/cdn-cgi/challenge-platform/%'
              AND NOT EXISTS (SELECT 1 FROM api_fetch_failure f WHERE f.response_body_id = body_id)
              AND NOT EXISTS (SELECT 1 FROM api_response_cache c WHERE c.body_id = body_id)"""
        .update.run()
      updated
    }
  }

  private def sha256(input: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes  = digest.digest(input.getBytes(StandardCharsets.UTF_8))
    bytes.map(b => String.format("%02x", b)).mkString
  }
}
