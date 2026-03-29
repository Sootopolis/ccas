-- Migration: database normalisation improvements
-- Date: 2026-03-29
-- Run against the production database BEFORE deploying the new code.
-- All statements are idempotent and safe to re-run.

BEGIN;

-- 1. Add missing FK: recruitment_blacklist.player_id -> player
ALTER TABLE recruitment_blacklist
  ADD CONSTRAINT recruitment_blacklist_player_id_fkey
  FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT;

-- 2. Add missing FK: player_recruitment_cache.player_id -> player
ALTER TABLE player_recruitment_cache
  ADD CONSTRAINT player_recruitment_cache_player_id_fkey
  FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT;

-- 3. Add UNIQUE index on club.slug
CREATE UNIQUE INDEX IF NOT EXISTS club_slug_key ON club (slug);

-- 4. job_run: replace club_slug TEXT with club_id BIGINT FK
ALTER TABLE job_run DROP COLUMN IF EXISTS club_slug;
ALTER TABLE job_run ADD COLUMN IF NOT EXISTS club_id BIGINT REFERENCES club (club_id);

-- 5. job_schedule: replace club_slug TEXT with club_id BIGINT FK + unique constraint
ALTER TABLE job_schedule DROP COLUMN IF EXISTS club_slug;
ALTER TABLE job_schedule ADD COLUMN IF NOT EXISTS club_id BIGINT REFERENCES club (club_id);
CREATE UNIQUE INDEX IF NOT EXISTS job_schedule_kind_club_id_key ON job_schedule (kind, club_id);

-- 6. Add job_run_id column to analysis run tables
ALTER TABLE membership_run  ADD COLUMN IF NOT EXISTS job_run_id TEXT;
ALTER TABLE recruitment_run  ADD COLUMN IF NOT EXISTS job_run_id TEXT;
ALTER TABLE history_run      ADD COLUMN IF NOT EXISTS job_run_id TEXT;

COMMIT;
