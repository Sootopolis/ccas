package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class ClientStats(
  sessionId: String,
  appLabel: String,
  configId: Long,
  startedAt: Instant,
  completedAt: Instant,
  requestsPerSec: Double,
  activeMs: Long,
  requests: Long,
  successes: Long,
  failures: Long,
  attempts: Long,
  errors429: Long,
  errors403: Long,
  errorsCf403: Long,
  errors404: Long,
  connectionErrors: Long,
  throttleDowns: Long,
  throttledMs: Long,
  currentPermits: Int,
  peakConcurrent: Int,
  gateWaitMs: Long,
  emaDelayMs: Long,
  latencyMinMs: Long,
  latencyMaxMs: Long,
  latencyMeanMs: Long,
  latencyBucket0to50: Long,
  latencyBucket50to100: Long,
  latencyBucket100to200: Long,
  latencyBucket200to500: Long,
  latencyBucket500to1000: Long,
  latencyBucket1000plus: Long
) derives DbCodec

object ClientStats {

  private val selectCols = SqlLiteral(
    """session_id, app_label, config_id, started_at, completed_at,
       requests_per_sec, active_ms,
       requests, successes, failures, attempts,
       errors_429, errors_403, errors_cf_403, errors_404, connection_errors,
       throttle_downs, throttled_ms, current_permits, peak_concurrent,
       gate_wait_ms, ema_delay_ms,
       latency_min_ms, latency_max_ms, latency_mean_ms,
       latency_bucket_0_50, latency_bucket_50_100, latency_bucket_100_200,
       latency_bucket_200_500, latency_bucket_500_1000, latency_bucket_1000_plus"""
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS client_stats (
              id                       BIGSERIAL PRIMARY KEY,
              session_id               TEXT NOT NULL,
              app_label                TEXT NOT NULL,
              config_id                BIGINT NOT NULL,
              started_at               TIMESTAMPTZ NOT NULL,
              completed_at             TIMESTAMPTZ NOT NULL,
              requests_per_sec         DOUBLE PRECISION NOT NULL,
              active_ms                BIGINT NOT NULL,
              requests                 BIGINT NOT NULL,
              successes                BIGINT NOT NULL,
              failures                 BIGINT NOT NULL,
              attempts                 BIGINT NOT NULL,
              errors_429               BIGINT NOT NULL,
              errors_403               BIGINT NOT NULL,
              errors_cf_403            BIGINT NOT NULL,
              errors_404               BIGINT NOT NULL,
              connection_errors        BIGINT NOT NULL,
              throttle_downs           BIGINT NOT NULL,
              throttled_ms             BIGINT NOT NULL,
              current_permits          INT NOT NULL,
              peak_concurrent          INT NOT NULL,
              gate_wait_ms             BIGINT NOT NULL,
              ema_delay_ms             BIGINT NOT NULL,
              latency_min_ms           BIGINT NOT NULL,
              latency_max_ms           BIGINT NOT NULL,
              latency_mean_ms          BIGINT NOT NULL,
              latency_bucket_0_50      BIGINT NOT NULL,
              latency_bucket_50_100    BIGINT NOT NULL,
              latency_bucket_100_200   BIGINT NOT NULL,
              latency_bucket_200_500   BIGINT NOT NULL,
              latency_bucket_500_1000  BIGINT NOT NULL,
              latency_bucket_1000_plus BIGINT NOT NULL,
              FOREIGN KEY (config_id) REFERENCES client_config (config_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_client_stats_session_id
            ON client_stats (session_id)""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_client_stats_started_at
            ON client_stats (started_at)""".update.run()
    }

  def insert(item: ClientStats): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO client_stats (
              session_id, app_label, config_id, started_at, completed_at,
              requests_per_sec, active_ms,
              requests, successes, failures, attempts,
              errors_429, errors_403, errors_cf_403, errors_404, connection_errors,
              throttle_downs, throttled_ms, current_permits, peak_concurrent,
              gate_wait_ms, ema_delay_ms,
              latency_min_ms, latency_max_ms, latency_mean_ms,
              latency_bucket_0_50, latency_bucket_50_100, latency_bucket_100_200,
              latency_bucket_200_500, latency_bucket_500_1000, latency_bucket_1000_plus
            ) VALUES (
              ${item.sessionId}, ${item.appLabel}, ${item.configId},
              ${item.startedAt}, ${item.completedAt},
              ${item.requestsPerSec}, ${item.activeMs},
              ${item.requests}, ${item.successes}, ${item.failures}, ${item.attempts},
              ${item.errors429}, ${item.errors403}, ${item.errorsCf403}, ${item.errors404}, ${item.connectionErrors},
              ${item.throttleDowns}, ${item.throttledMs}, ${item.currentPermits}, ${item.peakConcurrent},
              ${item.gateWaitMs}, ${item.emaDelayMs},
              ${item.latencyMinMs}, ${item.latencyMaxMs}, ${item.latencyMeanMs},
              ${item.latencyBucket0to50}, ${item.latencyBucket50to100}, ${item.latencyBucket100to200},
              ${item.latencyBucket200to500}, ${item.latencyBucket500to1000}, ${item.latencyBucket1000plus}
            )""".update.run()
    }

  def selectRecent(since: Instant): ZIO[PostgresClient, SQLException, List[ClientStats]] =
    connectZIO {
      sql"""SELECT $selectCols
            FROM client_stats WHERE started_at >= $since
            ORDER BY started_at DESC"""
        .query[ClientStats].run().toList
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
