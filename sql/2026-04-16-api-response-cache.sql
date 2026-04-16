-- Persistent cache of Chess.com API responses keyed by URL. Holds cache-control metadata (ETag, Last-Modified,
-- max-age, Content-Type) alongside a foreign key into api_response_body for the body itself. Body storage is
-- deduped by SHA-256 hash in api_response_body, so identical bodies across different URLs share one row.
--
-- Used by ChessComClient.getCacheable to serve max-age fresh hits and 304 revalidations without re-downloading.

CREATE TABLE IF NOT EXISTS api_response_cache (
  cache_id        BIGSERIAL PRIMARY KEY,
  fetched_at      TIMESTAMPTZ NOT NULL,
  url             TEXT NOT NULL UNIQUE,
  content_type    TEXT,
  max_age_seconds BIGINT,
  etag            TEXT,
  last_modified   TIMESTAMPTZ,
  body_id         BIGINT NOT NULL REFERENCES api_response_body (body_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_api_response_cache_fetched_at
  ON api_response_cache (fetched_at);
