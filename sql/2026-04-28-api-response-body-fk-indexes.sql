-- Index the foreign-key columns referencing api_response_body (body_id).
-- PostgreSQL does not auto-index FK columns (only PKs and uniques); without these,
-- ApiResponseCache.deleteOrphans (NOT EXISTS anti-joins from api_response_body
-- against api_response_cache.body_id and api_fetch_failure.response_body_id) and
-- the ON DELETE RESTRICT enforcement on every cache insert do sequential scans.
--
-- Apply during downtime or pre-traffic. Plain CREATE INDEX takes a SHARE lock on
-- the table for the duration of the build, blocking writes. On a live serving DB
-- swap each statement for CREATE INDEX CONCURRENTLY and run them outside any
-- enclosing transaction (psql -1 / BEGIN won't work with CONCURRENTLY).

CREATE INDEX IF NOT EXISTS idx_api_response_cache_body_id
  ON api_response_cache (body_id);

CREATE INDEX IF NOT EXISTS idx_api_fetch_failure_response_body_id
  ON api_fetch_failure (response_body_id);
