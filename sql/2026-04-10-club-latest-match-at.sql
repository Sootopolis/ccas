-- Replaces the unreliable Chess.com `last_activity` API field with a derived
-- `latest_match_at` populated by ClubDataApp from match data (DB-first, API
-- fallback). NULL is treated as "active / unknown" by the recruitment filter.
ALTER TABLE club DROP COLUMN IF EXISTS last_activity;
ALTER TABLE club ADD COLUMN IF NOT EXISTS latest_match_at TIMESTAMPTZ;
