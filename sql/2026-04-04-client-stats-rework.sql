-- Drop and recreate client_stats with a separate client_config table
-- and new diagnostic timing columns.
-- Existing client_stats data has limited value (survivorship-biased latency,
-- redundant config on every row) so we drop rather than migrate.

DROP TABLE IF EXISTS client_stats;

CREATE TABLE IF NOT EXISTS client_config (
  config_id            BIGSERIAL PRIMARY KEY,
  permits              INT NOT NULL,
  cooldown_secs        INT NOT NULL,
  cf_cooldown_secs     INT NOT NULL,
  retry_base_secs      INT NOT NULL,
  single_retry_secs    INT NOT NULL,
  cf_retry_secs        INT NOT NULL,
  failure_window_size  INT NOT NULL,
  failure_threshold    DOUBLE PRECISION NOT NULL,
  min_sample_size      INT NOT NULL
);

CREATE TABLE IF NOT EXISTS client_stats (
  id                BIGSERIAL PRIMARY KEY,
  app_label         TEXT NOT NULL,
  started_at        TIMESTAMPTZ NOT NULL,
  completed_at      TIMESTAMPTZ NOT NULL,
  config_id         BIGINT NOT NULL REFERENCES client_config (config_id) ON DELETE RESTRICT,
  requests_per_sec  DOUBLE PRECISION NOT NULL,
  requests          BIGINT NOT NULL,
  successes         BIGINT NOT NULL,
  failures          BIGINT NOT NULL,
  attempts          BIGINT NOT NULL,
  errors_429        BIGINT NOT NULL,
  errors_403        BIGINT NOT NULL,
  errors_404        BIGINT NOT NULL,
  connection_errors BIGINT NOT NULL,
  throttle_downs    BIGINT NOT NULL,
  throttled_ms      BIGINT NOT NULL,
  peak_concurrent   INT NOT NULL,
  gate_wait_ms      BIGINT NOT NULL,
  ema_delay_ms      BIGINT NOT NULL,
  latency_min_ms    BIGINT NOT NULL,
  latency_max_ms    BIGINT NOT NULL,
  latency_mean_ms   BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_client_stats_started_at ON client_stats (started_at);
