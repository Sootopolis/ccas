package ccas.analysis.tables

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

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

  def ensureBody(body: String): ZIO[PostgresClient, SQLException, Long] = {
    val hash = sha256(body)
    connectZIO {
      sql"SELECT body_id FROM api_response_body WHERE body_hash = $hash"
        .query[Long].run().headOption
    }.flatMap {
      case Some(id) => ZIO.succeed(id)
      case None => connectZIO {
        sql"""INSERT INTO api_response_body (body_hash, body) VALUES ($hash, $body)
              ON CONFLICT (body_hash) DO NOTHING""".update.run()
        sql"SELECT body_id FROM api_response_body WHERE body_hash = $hash".query[Long].run().head
      }
    }
  }

  def deleteOrphans: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""DELETE FROM api_response_body b
            WHERE NOT EXISTS (
              SELECT 1 FROM api_fetch_failure f WHERE f.response_body_id = b.body_id
            )""".update.run()
    }

  def deleteAll: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM api_response_body".update.run()
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
              AND NOT EXISTS (SELECT 1 FROM api_fetch_failure f WHERE f.response_body_id = body_id)"""
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
