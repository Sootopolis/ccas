-- Track how often each HistoryApp skip site short-circuited on an Unchanged CacheableResult (commit 1dddec6).
-- The existing client_stats cache counters measure transport-level savings (bytes off the wire); these three
-- measure domain-level savings (downstream DB / decode work avoided). Per-site columns preserve attribution
-- so we can tell which of the three refactor cases is paying off.

ALTER TABLE history_run ADD COLUMN IF NOT EXISTS refresh_match_unchanged       INT NOT NULL DEFAULT 0;
ALTER TABLE history_run ADD COLUMN IF NOT EXISTS seed_club_matches_unchanged   INT NOT NULL DEFAULT 0;
ALTER TABLE history_run ADD COLUMN IF NOT EXISTS seed_player_matches_unchanged INT NOT NULL DEFAULT 0;
