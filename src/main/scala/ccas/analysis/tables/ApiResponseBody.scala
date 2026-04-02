package ccas.analysis.tables

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class ApiResponseBody(
  bodyHash: String,
  body: String
) derives DbCodec

object ApiResponseBody {

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
      sql"INSERT INTO api_response_body (body_hash, body) VALUES ($hash, $body) ON CONFLICT (body_hash) DO NOTHING"
        .update.run()
      sql"SELECT body_id FROM api_response_body WHERE body_hash = $hash".query[Long].run().head
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

  private def sha256(input: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes  = digest.digest(input.getBytes(StandardCharsets.UTF_8))
    bytes.map(b => String.format("%02x", b)).mkString
  }
}
