-- Add config_hash column to client_config for content-based deduplication.
-- Backfill existing rows with SHA-256 of pipe-delimited config values,
-- matching the Scala-side computation in ClientConfig.computeHash.

ALTER TABLE client_config ADD COLUMN config_hash TEXT;

UPDATE client_config SET config_hash = encode(
  sha256(
    (permits || '|' || cooldown_secs || '|' || cf_cooldown_secs || '|' ||
     retry_base_secs || '|' || single_retry_secs || '|' || cf_retry_secs || '|' ||
     failure_window_size || '|' || failure_threshold || '|' || min_sample_size)::bytea
  ), 'hex'
);

ALTER TABLE client_config ALTER COLUMN config_hash SET NOT NULL;
ALTER TABLE client_config ADD CONSTRAINT client_config_hash_unique UNIQUE (config_hash);
