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
  configId: Long,
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
  latencyMinMs: Long,
  latencyMaxMs: Long,
  latencyMeanMs: Long,
  gateWaitMs: Long,
  emaDelayMs: Long,
  throttledMs: Long,
  secsPerRequest: Double
) derives DbCodec

object ClientStats {

  private val selectCols = SqlLiteral(
    """app_label, started_at, completed_at, config_id,
       requests, successes, failures, attempts,
       errors_429, errors_403, errors_404, connection_errors,
       throttle_downs, peak_concurrent,
       latency_min_ms, latency_max_ms, latency_mean_ms,
       gate_wait_ms, ema_delay_ms, throttled_ms,
       secs_per_request"""
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS client_stats (
              id                BIGSERIAL PRIMARY KEY,
              app_label         TEXT NOT NULL,
              started_at        TIMESTAMPTZ NOT NULL,
              completed_at      TIMESTAMPTZ NOT NULL,
              config_id         BIGINT NOT NULL,
              requests          BIGINT NOT NULL,
              successes         BIGINT NOT NULL,
              failures          BIGINT NOT NULL,
              attempts          BIGINT NOT NULL,
              errors_429        BIGINT NOT NULL,
              errors_403        BIGINT NOT NULL,
              errors_404        BIGINT NOT NULL,
              connection_errors BIGINT NOT NULL,
              throttle_downs    BIGINT NOT NULL,
              peak_concurrent   INT NOT NULL,
              latency_min_ms    BIGINT NOT NULL,
              latency_max_ms    BIGINT NOT NULL,
              latency_mean_ms   BIGINT NOT NULL,
              gate_wait_ms      BIGINT NOT NULL,
              ema_delay_ms      BIGINT NOT NULL,
              throttled_ms      BIGINT NOT NULL,
              secs_per_request  DOUBLE PRECISION NOT NULL,
              FOREIGN KEY (config_id) REFERENCES client_config (config_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_client_stats_started_at
            ON client_stats (started_at)""".update.run()
    }

  def insert(item: ClientStats): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO client_stats (
              app_label, started_at, completed_at, config_id,
              requests, successes, failures, attempts,
              errors_429, errors_403, errors_404, connection_errors,
              throttle_downs, peak_concurrent,
              latency_min_ms, latency_max_ms, latency_mean_ms,
              gate_wait_ms, ema_delay_ms, throttled_ms,
              secs_per_request
            ) VALUES (
              ${item.appLabel}, ${item.startedAt}, ${item.completedAt}, ${item.configId},
              ${item.requests}, ${item.successes}, ${item.failures}, ${item.attempts},
              ${item.errors429}, ${item.errors403}, ${item.errors404}, ${item.connectionErrors},
              ${item.throttleDowns}, ${item.peakConcurrent},
              ${item.latencyMinMs}, ${item.latencyMaxMs}, ${item.latencyMeanMs},
              ${item.gateWaitMs}, ${item.emaDelayMs}, ${item.throttledMs},
              ${item.secsPerRequest}
            )""".update.run()
    }

  def selectRecent(since: Instant): ZIO[PostgresClient, SQLException, List[ClientStats]] =
    connectZIO {
      sql"""SELECT $selectCols
            FROM client_stats WHERE started_at >= $since
            ORDER BY started_at DESC"""
        .query[ClientStats].run().toList
    }

  def insertReturningId(item: ClientStats): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO client_stats (
              app_label, started_at, completed_at, config_id,
              requests, successes, failures, attempts,
              errors_429, errors_403, errors_404, connection_errors,
              throttle_downs, peak_concurrent,
              latency_min_ms, latency_max_ms, latency_mean_ms,
              gate_wait_ms, ema_delay_ms, throttled_ms,
              secs_per_request
            ) VALUES (
              ${item.appLabel}, ${item.startedAt}, ${item.completedAt}, ${item.configId},
              ${item.requests}, ${item.successes}, ${item.failures}, ${item.attempts},
              ${item.errors429}, ${item.errors403}, ${item.errors404}, ${item.connectionErrors},
              ${item.throttleDowns}, ${item.peakConcurrent},
              ${item.latencyMinMs}, ${item.latencyMaxMs}, ${item.latencyMeanMs},
              ${item.gateWaitMs}, ${item.emaDelayMs}, ${item.throttledMs},
              ${item.secsPerRequest}
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
              latency_mean_ms = ${item.latencyMeanMs},
              gate_wait_ms = ${item.gateWaitMs}, ema_delay_ms = ${item.emaDelayMs},
              throttled_ms = ${item.throttledMs},
              secs_per_request = ${item.secsPerRequest}
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
