-- Rework client_stats from time-series (one row per flush window) to cumulative
-- upsert (one row per session). Drops errors403 column (non-CF 403s are not
-- actionable). Adds UNIQUE constraint on session_id for ON CONFLICT upsert.
-- Column order now groups: identity, timing, throughput, retries, errors,
-- throttling, overhead, latency.
--
-- client_config: recovery_tiers changed from pipe-delimited TEXT to INTEGER[].
--
-- Both tables are purely diagnostic, so we drop and recreate. Run once per environment.

DROP TABLE IF EXISTS client_stats;
DROP TABLE IF EXISTS client_config;

CREATE TABLE client_config (
  config_id                  BIGSERIAL PRIMARY KEY,
  config_hash                TEXT NOT NULL UNIQUE,
  recovery_tiers             INTEGER[] NOT NULL,
  cooldown_secs              INT NOT NULL,
  cf_cooldown_secs           INT NOT NULL,
  retry_base_secs            INT NOT NULL,
  cf_retry_secs              INT NOT NULL,
  connection_retry_base_secs INT NOT NULL,
  max429_retries             INT NOT NULL,
  max_cf_retries             INT NOT NULL,
  max_connection_retries     INT NOT NULL,
  failure_window_size        INT NOT NULL,
  failure_threshold          DOUBLE PRECISION NOT NULL,
  min_sample_size            INT NOT NULL
);

CREATE TABLE client_stats (
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
);

CREATE INDEX idx_client_stats_started_at ON client_stats (started_at);
