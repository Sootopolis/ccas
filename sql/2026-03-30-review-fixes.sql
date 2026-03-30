-- Database review fixes (2026-03-30)
-- All statements are idempotent.

-- 1. Drop redundant index (covered by idx_rc_player_outcome_eval)
DROP INDEX IF EXISTS idx_rc_player_id;

-- 2. Replace username index with composite (username, since DESC)
DROP INDEX IF EXISTS idx_player_snapshot_username;
CREATE INDEX IF NOT EXISTS idx_player_snapshot_username ON player_snapshot (username, since DESC);

-- 3. Replace single-column history_run index with composite
DROP INDEX IF EXISTS idx_history_run_club_id;
CREATE INDEX IF NOT EXISTS idx_history_run_club_started ON history_run (club_id, started_at DESC);

-- 4. Add missing FK: recruitment_run.criteria_id -> recruitment_criteria
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE constraint_name = 'recruitment_run_criteria_id_fkey'
      AND table_name = 'recruitment_run'
  ) THEN
    ALTER TABLE recruitment_run
      ADD CONSTRAINT recruitment_run_criteria_id_fkey
      FOREIGN KEY (criteria_id) REFERENCES recruitment_criteria (criteria_id);
  END IF;
END $$;
