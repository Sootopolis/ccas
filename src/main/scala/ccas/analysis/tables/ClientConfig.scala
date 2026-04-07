package ccas.analysis.tables

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class ClientConfig(
  configId: Long,
  configHash: String,
  // concurrency & pacing
  recoveryTiers: List[Int],
  minRequestDelayMs: Long,
  // recovery timing
  cooldownSecs: Int,
  cfCooldownSecs: Int,
  minTierObservationSecs: Int,
  // failure window
  failureWindowSize: Int,
  failureThreshold: Double,
  minSampleSize: Int,
  // retry timing
  retryBaseSecs: Int,
  cfRetrySecs: Int,
  connectionRetryBaseSecs: Int,
  // retry limits
  max429Retries: Int,
  maxCfRetries: Int,
  maxConnectionRetries: Int
) derives DbCodec {

  def computeHash: String = {
    val canonical =
      s"${recoveryTiers.mkString(",")}|$minRequestDelayMs|$cooldownSecs|$cfCooldownSecs|$minTierObservationSecs|$failureWindowSize|$failureThreshold|$minSampleSize|$retryBaseSecs|$cfRetrySecs|$connectionRetryBaseSecs|$max429Retries|$maxCfRetries|$maxConnectionRetries"
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes  = digest.digest(canonical.getBytes(StandardCharsets.UTF_8))
    bytes.map(b => String.format("%02x", b)).mkString
  }
}

object ClientConfig {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS client_config (
              config_id                  BIGSERIAL PRIMARY KEY,
              config_hash                TEXT NOT NULL UNIQUE,
              recovery_tiers             INTEGER[] NOT NULL,
              min_request_delay_ms       BIGINT NOT NULL,
              cooldown_secs              INT NOT NULL,
              cf_cooldown_secs           INT NOT NULL,
              min_tier_observation_secs  INT NOT NULL,
              failure_window_size        INT NOT NULL,
              failure_threshold          DOUBLE PRECISION NOT NULL,
              min_sample_size            INT NOT NULL,
              retry_base_secs            INT NOT NULL,
              cf_retry_secs              INT NOT NULL,
              connection_retry_base_secs INT NOT NULL,
              max429_retries             INT NOT NULL,
              max_cf_retries             INT NOT NULL,
              max_connection_retries     INT NOT NULL
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
                config_hash, recovery_tiers, min_request_delay_ms,
                cooldown_secs, cf_cooldown_secs, min_tier_observation_secs,
                failure_window_size, failure_threshold, min_sample_size,
                retry_base_secs, cf_retry_secs, connection_retry_base_secs,
                max429_retries, max_cf_retries, max_connection_retries
              ) VALUES (
                $hash, ${item.recoveryTiers}, ${item.minRequestDelayMs},
                ${item.cooldownSecs}, ${item.cfCooldownSecs}, ${item.minTierObservationSecs},
                ${item.failureWindowSize}, ${item.failureThreshold}, ${item.minSampleSize},
                ${item.retryBaseSecs}, ${item.cfRetrySecs}, ${item.connectionRetryBaseSecs},
                ${item.max429Retries}, ${item.maxCfRetries}, ${item.maxConnectionRetries}
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
