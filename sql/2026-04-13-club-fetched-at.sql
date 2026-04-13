-- Track when ClubDataApp last successfully refreshed each club's profile data.
-- Name mirrors club_match.fetched_at / player_recruitment_cache.fetched_at for consistency.
-- Used by the --min-age [hours] CLI flag to skip recently-processed clubs.
ALTER TABLE club ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ;
