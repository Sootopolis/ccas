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
  // identity
  sessionId: String,
  appLabel: String,
  configId: Long,
  startedAt: Instant,
  completedAt: Instant,
  // throughput
  requests: Long,
  successes: Long,
  failures: Long,
  attempts: Long,
  attemptsByTier: String,
  // errors
  errors429: Long,
  errors429ByTier: String,
  errorsCf403: Long,
  errorsCf403ByTier: String,
  errorsOther: Long,
  connectionErrors: Long,
  // throttle
  throttleDowns: Long,
  throttledMs: Long,
  peakConcurrent: Int,
  // overhead
  activeMs: Long,
  gateWaitMs: Long,
  emaDelayMs: Long,
  // latency
  latencyMinMs: Long,
  latencyMaxMs: Long,
  latencyMeanMs: Long,
  latencyBucket0To50: Long,
  latencyBucket50To100: Long,
  latencyBucket100To200: Long,
  latencyBucket200To500: Long,
  latencyBucket500To1000: Long,
  latencyBucket1000Plus: Long,
  // cache
  cacheHits: Long,
  cacheRevalidations: Long,
  cacheMisses: Long
) derives DbCodec

object ClientStats {

  private val allCols = SqlLiteral(
    """session_id, app_label, config_id, started_at, completed_at,
       requests, successes, failures, attempts, attempts_by_tier,
       errors429, errors429_by_tier, errors_cf403, errors_cf403_by_tier, errors_other, connection_errors,
       throttle_downs, throttled_ms, peak_concurrent,
       active_ms, gate_wait_ms, ema_delay_ms,
       latency_min_ms, latency_max_ms, latency_mean_ms,
       latency_bucket0_to50, latency_bucket50_to100, latency_bucket100_to200,
       latency_bucket200_to500, latency_bucket500_to1000, latency_bucket1000_plus,
       cache_hits, cache_revalidations, cache_misses"""
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS client_stats (
              session_id               TEXT PRIMARY KEY,
              app_label                TEXT NOT NULL,
              config_id                BIGINT NOT NULL,
              started_at               TIMESTAMPTZ NOT NULL,
              completed_at             TIMESTAMPTZ NOT NULL,
              requests                 BIGINT NOT NULL,
              successes                BIGINT NOT NULL,
              failures                 BIGINT NOT NULL,
              attempts                 BIGINT NOT NULL,
              attempts_by_tier         TEXT NOT NULL,
              errors429                BIGINT NOT NULL,
              errors429_by_tier        TEXT NOT NULL,
              errors_cf403             BIGINT NOT NULL,
              errors_cf403_by_tier    TEXT NOT NULL,
              errors_other             BIGINT NOT NULL,
              connection_errors        BIGINT NOT NULL,
              throttle_downs           BIGINT NOT NULL,
              throttled_ms             BIGINT NOT NULL,
              peak_concurrent          INT NOT NULL,
              active_ms                BIGINT NOT NULL,
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
              cache_hits               BIGINT NOT NULL DEFAULT 0,
              cache_revalidations      BIGINT NOT NULL DEFAULT 0,
              cache_misses             BIGINT NOT NULL DEFAULT 0,
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
              ${item.requests}, ${item.successes}, ${item.failures},
              ${item.attempts}, ${item.attemptsByTier},
              ${item.errors429}, ${item.errors429ByTier},
              ${item.errorsCf403}, ${item.errorsCf403ByTier},
              ${item.errorsOther}, ${item.connectionErrors},
              ${item.throttleDowns}, ${item.throttledMs}, ${item.peakConcurrent},
              ${item.activeMs}, ${item.gateWaitMs}, ${item.emaDelayMs},
              ${item.latencyMinMs}, ${item.latencyMaxMs}, ${item.latencyMeanMs},
              ${item.latencyBucket0To50}, ${item.latencyBucket50To100}, ${item.latencyBucket100To200},
              ${item.latencyBucket200To500}, ${item.latencyBucket500To1000}, ${item.latencyBucket1000Plus},
              ${item.cacheHits}, ${item.cacheRevalidations}, ${item.cacheMisses}
            ) ON CONFLICT (session_id) DO UPDATE SET
              completed_at = EXCLUDED.completed_at,
              requests = EXCLUDED.requests,
              successes = EXCLUDED.successes,
              failures = EXCLUDED.failures,
              attempts = EXCLUDED.attempts,
              attempts_by_tier = EXCLUDED.attempts_by_tier,
              errors429 = EXCLUDED.errors429,
              errors429_by_tier = EXCLUDED.errors429_by_tier,
              errors_cf403 = EXCLUDED.errors_cf403,
              errors_cf403_by_tier = EXCLUDED.errors_cf403_by_tier,
              errors_other = EXCLUDED.errors_other,
              connection_errors = EXCLUDED.connection_errors,
              throttle_downs = EXCLUDED.throttle_downs,
              throttled_ms = EXCLUDED.throttled_ms,
              peak_concurrent = EXCLUDED.peak_concurrent,
              active_ms = EXCLUDED.active_ms,
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
              latency_bucket1000_plus = EXCLUDED.latency_bucket1000_plus,
              cache_hits = EXCLUDED.cache_hits,
              cache_revalidations = EXCLUDED.cache_revalidations,
              cache_misses = EXCLUDED.cache_misses
            """.update.run()
    }

  def selectRecent(since: Instant): ZIO[PostgresClient, SQLException, List[ClientStats]] =
    connectZIO {
      sql"""SELECT $allCols
            FROM client_stats WHERE started_at >= $since
            ORDER BY started_at DESC"""
        .query[ClientStats].run().toList
    }
}
