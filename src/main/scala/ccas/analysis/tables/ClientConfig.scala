package ccas.analysis.tables

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class ClientConfig(
  configId: Long,
  configHash: String,
  permits: Int,
  cooldownSecs: Int,
  cfCooldownSecs: Int,
  retryBaseSecs: Int,
  singleRetrySecs: Int,
  cfRetrySecs: Int,
  failureWindowSize: Int,
  failureThreshold: Double,
  minSampleSize: Int
) derives DbCodec {

  def computeHash: String = {
    val canonical =
      s"$permits|$cooldownSecs|$cfCooldownSecs|$retryBaseSecs|$singleRetrySecs|$cfRetrySecs|$failureWindowSize|$failureThreshold|$minSampleSize"
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes  = digest.digest(canonical.getBytes(StandardCharsets.UTF_8))
    bytes.map(b => String.format("%02x", b)).mkString
  }
}

object ClientConfig {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS client_config (
              config_id            BIGSERIAL PRIMARY KEY,
              config_hash          TEXT NOT NULL UNIQUE,
              permits              INT NOT NULL,
              cooldown_secs        INT NOT NULL,
              cf_cooldown_secs     INT NOT NULL,
              retry_base_secs      INT NOT NULL,
              single_retry_secs    INT NOT NULL,
              cf_retry_secs        INT NOT NULL,
              failure_window_size  INT NOT NULL,
              failure_threshold    DOUBLE PRECISION NOT NULL,
              min_sample_size      INT NOT NULL
            )""".update.run()
    }

  /** Return the config_id for this set of values, inserting a new row only if no match exists. */
  def ensureConfig(item: ClientConfig): ZIO[PostgresClient, SQLException, Long] = {
    val hash = item.configHash
    connectZIO {
      sql"SELECT config_id FROM client_config WHERE config_hash = $hash"
        .query[Long].run().headOption
    }.flatMap {
      case Some(id) => ZIO.succeed(id)
      case None => connectZIO {
        sql"""INSERT INTO client_config (
                config_hash, permits, cooldown_secs, cf_cooldown_secs,
                retry_base_secs, single_retry_secs, cf_retry_secs,
                failure_window_size, failure_threshold, min_sample_size
              ) VALUES (
                $hash, ${item.permits}, ${item.cooldownSecs}, ${item.cfCooldownSecs},
                ${item.retryBaseSecs}, ${item.singleRetrySecs}, ${item.cfRetrySecs},
                ${item.failureWindowSize}, ${item.failureThreshold}, ${item.minSampleSize}
              ) ON CONFLICT (config_hash) DO NOTHING""".update.run()
        sql"SELECT config_id FROM client_config WHERE config_hash = $hash".query[Long].run().head
      }
    }
  }

  def deleteAll: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM client_config".update.run()
    }
}
