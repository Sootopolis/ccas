-- Rework client_stats and client_config into time-series diagnostics.
--
-- Schema changes:
--   * client_stats is now time-bucketed: each flush inserts a new row representing that window's delta,
--     grouped by `session_id` (one session per client instance). Replaces the single-row-per-session model.
--   * New columns: session_id, active_ms, errors_cf_403, current_permits, latency histogram buckets.
--   * Removed columns: none (column order reorganized by logical group).
--   * client_config stores recovery_tiers (pipe-delimited, e.g. "2|4|6|8") in place of the single `permits` int.
--     The throttle recovery path is now a configurable ladder rather than hardcoded doubling.
--
-- Both tables are purely diagnostic, so we drop and recreate. Run once manually per environment.

DROP TABLE IF EXISTS client_stats;
DROP TABLE IF EXISTS client_config;

CREATE TABLE client_config (
  config_id                  BIGSERIAL PRIMARY KEY,
  config_hash                TEXT NOT NULL UNIQUE,
  recovery_tiers             TEXT NOT NULL,
  cooldown_secs              INT NOT NULL,
  cf_cooldown_secs           INT NOT NULL,
  retry_base_secs            INT NOT NULL,
  cf_retry_secs              INT NOT NULL,
  connection_retry_base_secs INT NOT NULL,
  max_429_retries            INT NOT NULL,
  max_cf_retries             INT NOT NULL,
  max_connection_retries     INT NOT NULL,
  failure_window_size        INT NOT NULL,
  failure_threshold          DOUBLE PRECISION NOT NULL,
  min_sample_size            INT NOT NULL
);

CREATE TABLE client_stats (
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
);

CREATE INDEX idx_client_stats_session_id ON client_stats (session_id);
CREATE INDEX idx_client_stats_started_at ON client_stats (started_at);
