package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
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
  attemptsByTier: String,
  errors429: Long,
  errors429ByTier: String,
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
  latencyBucket0To50: Long,
  latencyBucket50To100: Long,
  latencyBucket100To200: Long,
  latencyBucket200To500: Long,
  latencyBucket500To1000: Long,
  latencyBucket1000Plus: Long
) derives DbCodec

object ClientStats {

  private val allCols = SqlLiteral(
    """session_id, app_label, config_id, started_at, completed_at,
       requests_per_sec, active_ms,
       requests, successes, failures, attempts, attempts_by_tier,
       errors429, errors429_by_tier, errors_cf403, errors404, connection_errors,
       throttle_downs, throttled_ms, current_permits, peak_concurrent,
       gate_wait_ms, ema_delay_ms,
       latency_min_ms, latency_max_ms, latency_mean_ms,
       latency_bucket0_to50, latency_bucket50_to100, latency_bucket100_to200,
       latency_bucket200_to500, latency_bucket500_to1000, latency_bucket1000_plus"""
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS client_stats (
              id                       BIGSERIAL PRIMARY KEY,
              session_id               TEXT NOT NULL UNIQUE,
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
              attempts_by_tier         TEXT NOT NULL,
              errors429                BIGINT NOT NULL,
              errors429_by_tier        TEXT NOT NULL,
              errors_cf403             BIGINT NOT NULL,
              errors404                BIGINT NOT NULL,
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
              latency_bucket0_to50     BIGINT NOT NULL,
              latency_bucket50_to100   BIGINT NOT NULL,
              latency_bucket100_to200  BIGINT NOT NULL,
              latency_bucket200_to500  BIGINT NOT NULL,
              latency_bucket500_to1000 BIGINT NOT NULL,
              latency_bucket1000_plus  BIGINT NOT NULL,
              FOREIGN KEY (config_id) REFERENCES client_config (config_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_client_stats_started_at
            ON client_stats (started_at)""".update.run()
    }

  def upsert(item: ClientStats): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO client_stats (
              $allCols
            ) VALUES (
              ${item.sessionId}, ${item.appLabel}, ${item.configId},
              ${item.startedAt}, ${item.completedAt},
              ${item.requestsPerSec}, ${item.activeMs},
              ${item.requests}, ${item.successes}, ${item.failures},
              ${item.attempts}, ${item.attemptsByTier},
              ${item.errors429}, ${item.errors429ByTier},
              ${item.errorsCf403}, ${item.errors404}, ${item.connectionErrors},
              ${item.throttleDowns}, ${item.throttledMs}, ${item.currentPermits}, ${item.peakConcurrent},
              ${item.gateWaitMs}, ${item.emaDelayMs},
              ${item.latencyMinMs}, ${item.latencyMaxMs}, ${item.latencyMeanMs},
              ${item.latencyBucket0To50}, ${item.latencyBucket50To100}, ${item.latencyBucket100To200},
              ${item.latencyBucket200To500}, ${item.latencyBucket500To1000}, ${item.latencyBucket1000Plus}
            ) ON CONFLICT (session_id) DO UPDATE SET
              completed_at = EXCLUDED.completed_at,
              requests_per_sec = EXCLUDED.requests_per_sec,
              active_ms = EXCLUDED.active_ms,
              requests = EXCLUDED.requests,
              successes = EXCLUDED.successes,
              failures = EXCLUDED.failures,
              attempts = EXCLUDED.attempts,
              attempts_by_tier = EXCLUDED.attempts_by_tier,
              errors429 = EXCLUDED.errors429,
              errors429_by_tier = EXCLUDED.errors429_by_tier,
              errors_cf403 = EXCLUDED.errors_cf403,
              errors404 = EXCLUDED.errors404,
              connection_errors = EXCLUDED.connection_errors,
              throttle_downs = EXCLUDED.throttle_downs,
              throttled_ms = EXCLUDED.throttled_ms,
              current_permits = EXCLUDED.current_permits,
              peak_concurrent = EXCLUDED.peak_concurrent,
              gate_wait_ms = EXCLUDED.gate_wait_ms,
              ema_delay_ms = EXCLUDED.ema_delay_ms,
              latency_min_ms = EXCLUDED.latency_min_ms,
              latency_max_ms = EXCLUDED.latency_max_ms,
              latency_mean_ms = EXCLUDED.latency_mean_ms,
              latency_bucket0_to50 = EXCLUDED.latency_bucket0_to50,
              latency_bucket50_to100 = EXCLUDED.latency_bucket50_to100,
              latency_bucket100_to200 = EXCLUDED.latency_bucket100_to200,
              latency_bucket200_to500 = EXCLUDED.latency_bucket200_to500,
              latency_bucket500_to1000 = EXCLUDED.latency_bucket500_to1000,
              latency_bucket1000_plus = EXCLUDED.latency_bucket1000_plus
            """.update.run()
    }

  def selectRecent(since: Instant): ZIO[PostgresClient, SQLException, List[ClientStats]] =
    connectZIO {
      sql"""SELECT $allCols
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
