-- Track cache hits, 304 revalidations, and cache misses per client session, so operators can see the impact of
-- the new response cache at a glance. Counters live on ClientStats rather than a separate table so they roll up
-- inside the existing per-session snapshot.

ALTER TABLE client_stats ADD COLUMN IF NOT EXISTS cache_hits BIGINT NOT NULL DEFAULT 0;
ALTER TABLE client_stats ADD COLUMN IF NOT EXISTS cache_revalidations BIGINT NOT NULL DEFAULT 0;
ALTER TABLE client_stats ADD COLUMN IF NOT EXISTS cache_misses BIGINT NOT NULL DEFAULT 0;
