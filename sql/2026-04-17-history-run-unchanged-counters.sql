-- Track how often each HistoryApp skip site short-circuited on an Unchanged CacheableResult (commit 1dddec6).
-- The existing client_stats cache counters measure transport-level savings (bytes off the wire); these three
-- measure domain-level savings (downstream DB / decode work avoided). Per-site columns preserve attribution
-- so we can tell which of the three refactor cases is paying off.

ALTER TABLE history_run ADD COLUMN IF NOT EXISTS refresh_match_unchanged       INT NOT NULL DEFAULT 0;
ALTER TABLE history_run ADD COLUMN IF NOT EXISTS seed_club_matches_unchanged   INT NOT NULL DEFAULT 0;
ALTER TABLE history_run ADD COLUMN IF NOT EXISTS seed_player_matches_unchanged INT NOT NULL DEFAULT 0;

-- Roll-up query to evaluate the refactor's impact after a week of traffic:
--   SELECT COUNT(*)                             AS runs,
--          SUM(refresh_match_unchanged)         AS refresh_skips,
--          SUM(seed_club_matches_unchanged)     AS seed_club_skips,
--          SUM(seed_player_matches_unchanged)   AS seed_player_skips
--   FROM   history_run
--   WHERE  started_at > now() - interval '7 days';
