package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class ClientStats(
  appLabel: String,
  startedAt: Instant,
  completedAt: Instant,
  requests: Long,
  successes: Long,
  failures: Long,
  attempts: Long,
  errors429: Long,
  errors403: Long,
  errors404: Long,
  connectionErrors: Long,
  throttleDowns: Long,
  peakConcurrent: Int,
  configPermits: Int,
  configCooldownSecs: Int,
  configCfCooldownSecs: Int,
  configRetryBaseSecs: Int,
  config403RetrySecs: Int,
  configCfRetrySecs: Int,
  configFailureWindowSize: Int,
  configFailureThreshold: Double,
  configMinSampleSize: Int,
  latencyMinMs: Long,
  latencyMaxMs: Long,
  latencyMeanMs: Long
) derives DbCodec

object ClientStats {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS client_stats (
              id                       BIGSERIAL PRIMARY KEY,
              app_label                TEXT NOT NULL,
              started_at               TIMESTAMPTZ NOT NULL,
              completed_at             TIMESTAMPTZ NOT NULL,
              requests                 BIGINT NOT NULL,
              successes                BIGINT NOT NULL,
              failures                 BIGINT NOT NULL,
              attempts                 BIGINT NOT NULL,
              errors_429               BIGINT NOT NULL,
              errors_403               BIGINT NOT NULL,
              errors_404               BIGINT NOT NULL,
              connection_errors        BIGINT NOT NULL,
              throttle_downs           BIGINT NOT NULL,
              peak_concurrent          INT NOT NULL,
              config_permits           INT NOT NULL,
              config_cooldown_secs     INT NOT NULL,
              config_cf_cooldown_secs  INT NOT NULL,
              config_retry_base_secs   INT NOT NULL,
              config_403_retry_secs    INT NOT NULL,
              config_cf_retry_secs     INT NOT NULL,
              config_failure_window_size INT NOT NULL,
              config_failure_threshold DOUBLE PRECISION NOT NULL,
              config_min_sample_size   INT NOT NULL,
              latency_min_ms           BIGINT NOT NULL,
              latency_max_ms           BIGINT NOT NULL,
              latency_mean_ms          BIGINT NOT NULL
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_client_stats_started_at
            ON client_stats (started_at)""".update.run()
    }

  def insert(item: ClientStats): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO client_stats (
              app_label, started_at, completed_at,
              requests, successes, failures, attempts,
              errors_429, errors_403, errors_404, connection_errors,
              throttle_downs, peak_concurrent,
              config_permits, config_cooldown_secs, config_cf_cooldown_secs,
              config_retry_base_secs, config_403_retry_secs, config_cf_retry_secs,
              config_failure_window_size, config_failure_threshold, config_min_sample_size,
              latency_min_ms, latency_max_ms, latency_mean_ms
            ) VALUES (
              ${item.appLabel}, ${item.startedAt}, ${item.completedAt},
              ${item.requests}, ${item.successes}, ${item.failures}, ${item.attempts},
              ${item.errors429}, ${item.errors403}, ${item.errors404}, ${item.connectionErrors},
              ${item.throttleDowns}, ${item.peakConcurrent},
              ${item.configPermits}, ${item.configCooldownSecs}, ${item.configCfCooldownSecs},
              ${item.configRetryBaseSecs}, ${item.config403RetrySecs}, ${item.configCfRetrySecs},
              ${item.configFailureWindowSize}, ${item.configFailureThreshold}, ${item.configMinSampleSize},
              ${item.latencyMinMs}, ${item.latencyMaxMs}, ${item.latencyMeanMs}
            )""".update.run()
    }

  def selectRecent(since: Instant): ZIO[PostgresClient, SQLException, List[ClientStats]] =
    connectZIO {
      sql"""SELECT app_label, started_at, completed_at,
                   requests, successes, failures, attempts,
                   errors_429, errors_403, errors_404, connection_errors,
                   throttle_downs, peak_concurrent,
                   config_permits, config_cooldown_secs, config_cf_cooldown_secs,
                   config_retry_base_secs, config_403_retry_secs, config_cf_retry_secs,
                   config_failure_window_size, config_failure_threshold, config_min_sample_size,
                   latency_min_ms, latency_max_ms, latency_mean_ms
            FROM client_stats WHERE started_at >= $since
            ORDER BY started_at DESC"""
        .query[ClientStats].run().toList
    }

  def insertReturningId(item: ClientStats): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO client_stats (
              app_label, started_at, completed_at,
              requests, successes, failures, attempts,
              errors_429, errors_403, errors_404, connection_errors,
              throttle_downs, peak_concurrent,
              config_permits, config_cooldown_secs, config_cf_cooldown_secs,
              config_retry_base_secs, config_403_retry_secs, config_cf_retry_secs,
              config_failure_window_size, config_failure_threshold, config_min_sample_size,
              latency_min_ms, latency_max_ms, latency_mean_ms
            ) VALUES (
              ${item.appLabel}, ${item.startedAt}, ${item.completedAt},
              ${item.requests}, ${item.successes}, ${item.failures}, ${item.attempts},
              ${item.errors429}, ${item.errors403}, ${item.errors404}, ${item.connectionErrors},
              ${item.throttleDowns}, ${item.peakConcurrent},
              ${item.configPermits}, ${item.configCooldownSecs}, ${item.configCfCooldownSecs},
              ${item.configRetryBaseSecs}, ${item.config403RetrySecs}, ${item.configCfRetrySecs},
              ${item.configFailureWindowSize}, ${item.configFailureThreshold}, ${item.configMinSampleSize},
              ${item.latencyMinMs}, ${item.latencyMaxMs}, ${item.latencyMeanMs}
            ) RETURNING id""".query[Long].run().head
    }

  def updateById(id: Long, item: ClientStats): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE client_stats SET
              completed_at = ${item.completedAt},
              requests = ${item.requests}, successes = ${item.successes},
              failures = ${item.failures}, attempts = ${item.attempts},
              errors_429 = ${item.errors429}, errors_403 = ${item.errors403},
              errors_404 = ${item.errors404}, connection_errors = ${item.connectionErrors},
              throttle_downs = ${item.throttleDowns}, peak_concurrent = ${item.peakConcurrent},
              latency_min_ms = ${item.latencyMinMs}, latency_max_ms = ${item.latencyMaxMs},
              latency_mean_ms = ${item.latencyMeanMs}
            WHERE id = $id""".update.run()
    }

  def deleteBefore(cutoff: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM client_stats WHERE started_at < $cutoff".update.run()
    }

  def deleteAll: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM client_stats".update.run()
    }
}
