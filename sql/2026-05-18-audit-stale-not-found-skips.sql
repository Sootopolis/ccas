-- One-shot audit (not a schema migration): purge stale `RefSkipReason.NotFound` rows from
-- `player_ref_skip` / `club_ref_skip` whose originating fetch failure was a transient backend 404
-- (body `"An internal error has occurred…"`, codes 0 / 3024 / 403) rather than a canonical
-- `"X not found."`. Before issue #3, both body shapes wrote `NotFound`; only the latter is now
-- correct, so the historical rows for the transient class need clearing so the next resolution
-- run can re-attempt them under the `ApiError` path.
--
-- The split is observable via `api_fetch_failure.error_type`: `'ReportedNotFound'` for canonical
-- bodies, `'HttpStatusException'` for everything else. The discriminator was introduced
-- 2026-05-10 (commit 6c38b32). The 2026-05-11 cutoff below is one day past the commit date to
-- cover any same-day window where the old code was still running in prod — failures stamped
-- on 2026-05-10 morning under the old code would still stamp `'HttpStatusException'` regardless
-- of body and must not be wrongly purged. Tighten the cutoff if the prod deploy timestamp is
-- known precisely.
--
-- Performance: `api_fetch_failure.url` is not indexed (only `occurred_at` and `response_body_id`).
-- Without a temp index, the LATERAL lookup scans the table once per `NotFound` skip row. On a
-- large `api_fetch_failure` table this can take minutes. Create a temp index first if so:
--
--   CREATE INDEX CONCURRENTLY idx_api_fetch_failure_url_tmp ON api_fetch_failure (url, occurred_at DESC);
--   -- run audit
--   DROP INDEX CONCURRENTLY idx_api_fetch_failure_url_tmp;
--
-- Workflow:
--   1. Optional: create the temp index above if `player_ref_skip.reason = 'NotFound'` row count
--      is in the thousands.
--   2. Run the SELECT block. Record the counts in the PR description.
--   3. Review the candidate rows.
--   4. Uncomment the DELETE block and run.

-- ============================================================================
-- SELECT (read-only): count and list candidates
-- ============================================================================

\echo '--- Baseline NotFound skip counts ---'

SELECT 'player' AS table, count(*) AS not_found_rows FROM player_ref_skip WHERE reason = 'NotFound'
UNION ALL
SELECT 'club',           count(*)                  FROM club_ref_skip    WHERE reason = 'NotFound';

\echo '--- Player candidates (stale NotFound — most recent /player/<u>* failure is transient) ---'

SELECT
  p.player_id,
  p.username,
  latest.error_type   AS latest_error_type,
  latest.occurred_at  AS latest_occurred_at
FROM player_ref_skip prs
JOIN player p USING (player_id)
LEFT JOIN LATERAL (
  SELECT f.error_type, f.occurred_at
  FROM api_fetch_failure f
  WHERE f.url LIKE 'https://api.chess.com/pub/player/' || p.username
     OR f.url LIKE 'https://api.chess.com/pub/player/' || p.username || '/%'
  ORDER BY f.occurred_at DESC
  LIMIT 1
) AS latest ON TRUE
WHERE prs.reason = 'NotFound'
  AND latest.error_type = 'HttpStatusException'
  AND latest.occurred_at >= TIMESTAMPTZ '2026-05-11 00:00:00+00'
ORDER BY latest.occurred_at DESC;

\echo '--- Club candidates (stale NotFound — most recent /club/<slug>* failure is transient) ---'

SELECT
  c.club_id,
  c.slug,
  latest.error_type   AS latest_error_type,
  latest.occurred_at  AS latest_occurred_at
FROM club_ref_skip crs
JOIN club c USING (club_id)
LEFT JOIN LATERAL (
  SELECT f.error_type, f.occurred_at
  FROM api_fetch_failure f
  WHERE f.url LIKE 'https://api.chess.com/pub/club/' || c.slug
     OR f.url LIKE 'https://api.chess.com/pub/club/' || c.slug || '/%'
  ORDER BY f.occurred_at DESC
  LIMIT 1
) AS latest ON TRUE
WHERE crs.reason = 'NotFound'
  AND latest.error_type = 'HttpStatusException'
  AND latest.occurred_at >= TIMESTAMPTZ '2026-05-11 00:00:00+00'
ORDER BY latest.occurred_at DESC;

-- ============================================================================
-- DELETE (mutating): uncomment after reviewing the SELECT output above
-- ============================================================================

-- BEGIN;
--
-- DELETE FROM player_ref_skip prs
-- USING player p,
--   LATERAL (
--     SELECT f.error_type, f.occurred_at
--     FROM api_fetch_failure f
--     WHERE f.url LIKE 'https://api.chess.com/pub/player/' || p.username
--        OR f.url LIKE 'https://api.chess.com/pub/player/' || p.username || '/%'
--     ORDER BY f.occurred_at DESC
--     LIMIT 1
--   ) AS latest
-- WHERE prs.player_id = p.player_id
--   AND prs.reason = 'NotFound'
--   AND latest.error_type = 'HttpStatusException'
--   AND latest.occurred_at >= TIMESTAMPTZ '2026-05-11 00:00:00+00';
--
-- DELETE FROM club_ref_skip crs
-- USING club c,
--   LATERAL (
--     SELECT f.error_type, f.occurred_at
--     FROM api_fetch_failure f
--     WHERE f.url LIKE 'https://api.chess.com/pub/club/' || c.slug
--        OR f.url LIKE 'https://api.chess.com/pub/club/' || c.slug || '/%'
--     ORDER BY f.occurred_at DESC
--     LIMIT 1
--   ) AS latest
-- WHERE crs.club_id = c.club_id
--   AND crs.reason = 'NotFound'
--   AND latest.error_type = 'HttpStatusException'
--   AND latest.occurred_at >= TIMESTAMPTZ '2026-05-11 00:00:00+00';
--
-- COMMIT;
