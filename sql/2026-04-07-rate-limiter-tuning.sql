-- Add min-request-delay and min-tier-observation columns to client_config.
-- Backfill existing rows with the previous implicit defaults (0ms delay, 0s observation).
ALTER TABLE client_config ADD COLUMN min_request_delay_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE client_config ADD COLUMN min_tier_observation_secs INT NOT NULL DEFAULT 0;
-- Drop the defaults after backfill so future inserts must be explicit.
ALTER TABLE client_config ALTER COLUMN min_request_delay_ms DROP DEFAULT;
ALTER TABLE client_config ALTER COLUMN min_tier_observation_secs DROP DEFAULT;

-- Add per-tier CF 403 breakdown to client_stats.
ALTER TABLE client_stats ADD COLUMN errors_cf403_by_tier TEXT NOT NULL DEFAULT '';
ALTER TABLE client_stats ALTER COLUMN errors_cf403_by_tier DROP DEFAULT;
