-- Standardise FK constraints, add partial index, narrow integer columns (2026-03-30)
-- All statements are idempotent (DROP IF EXISTS + re-add, CREATE IF NOT EXISTS, ALTER TYPE is a no-op if already correct).

-- club_match
ALTER TABLE club_match DROP CONSTRAINT IF EXISTS club_match_team1_club_id_fkey;
ALTER TABLE club_match ADD CONSTRAINT club_match_team1_club_id_fkey
  FOREIGN KEY (team1_club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

ALTER TABLE club_match DROP CONSTRAINT IF EXISTS club_match_team2_club_id_fkey;
ALTER TABLE club_match ADD CONSTRAINT club_match_team2_club_id_fkey
  FOREIGN KEY (team2_club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

-- club_match_board (player FKs only; match_id FK stays ON DELETE CASCADE)
ALTER TABLE club_match_board DROP CONSTRAINT IF EXISTS club_match_board_team1_player_id_fkey;
ALTER TABLE club_match_board ADD CONSTRAINT club_match_board_team1_player_id_fkey
  FOREIGN KEY (team1_player_id) REFERENCES player (player_id) ON DELETE RESTRICT;

ALTER TABLE club_match_board DROP CONSTRAINT IF EXISTS club_match_board_team2_player_id_fkey;
ALTER TABLE club_match_board ADD CONSTRAINT club_match_board_team2_player_id_fkey
  FOREIGN KEY (team2_player_id) REFERENCES player (player_id) ON DELETE RESTRICT;

-- history_run
ALTER TABLE history_run DROP CONSTRAINT IF EXISTS history_run_club_id_fkey;
ALTER TABLE history_run ADD CONSTRAINT history_run_club_id_fkey
  FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

-- history_pending_match
ALTER TABLE history_pending_match DROP CONSTRAINT IF EXISTS history_pending_match_club_id_fkey;
ALTER TABLE history_pending_match ADD CONSTRAINT history_pending_match_club_id_fkey
  FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

-- history_member_query
ALTER TABLE history_member_query DROP CONSTRAINT IF EXISTS history_member_query_club_id_fkey;
ALTER TABLE history_member_query ADD CONSTRAINT history_member_query_club_id_fkey
  FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

ALTER TABLE history_member_query DROP CONSTRAINT IF EXISTS history_member_query_player_id_fkey;
ALTER TABLE history_member_query ADD CONSTRAINT history_member_query_player_id_fkey
  FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT;

-- club_match_ref
ALTER TABLE club_match_ref DROP CONSTRAINT IF EXISTS club_match_ref_club_id_fkey;
ALTER TABLE club_match_ref ADD CONSTRAINT club_match_ref_club_id_fkey
  FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

-- player_match_ref
ALTER TABLE player_match_ref DROP CONSTRAINT IF EXISTS player_match_ref_player_id_fkey;
ALTER TABLE player_match_ref ADD CONSTRAINT player_match_ref_player_id_fkey
  FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT;

-- player_tournament_ref
ALTER TABLE player_tournament_ref DROP CONSTRAINT IF EXISTS player_tournament_ref_player_id_fkey;
ALTER TABLE player_tournament_ref ADD CONSTRAINT player_tournament_ref_player_id_fkey
  FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT;

-- player_ref_skip
ALTER TABLE player_ref_skip DROP CONSTRAINT IF EXISTS player_ref_skip_player_id_fkey;
ALTER TABLE player_ref_skip ADD CONSTRAINT player_ref_skip_player_id_fkey
  FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT;

-- club_ref_skip
ALTER TABLE club_ref_skip DROP CONSTRAINT IF EXISTS club_ref_skip_club_id_fkey;
ALTER TABLE club_ref_skip ADD CONSTRAINT club_ref_skip_club_id_fkey
  FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

-- job_run
ALTER TABLE job_run DROP CONSTRAINT IF EXISTS job_run_club_id_fkey;
ALTER TABLE job_run ADD CONSTRAINT job_run_club_id_fkey
  FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

-- job_schedule
ALTER TABLE job_schedule DROP CONSTRAINT IF EXISTS job_schedule_club_id_fkey;
ALTER TABLE job_schedule ADD CONSTRAINT job_schedule_club_id_fkey
  FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT;

-- recruitment_run (criteria_id FK — fixes prior migration that omitted ON DELETE)
ALTER TABLE recruitment_run DROP CONSTRAINT IF EXISTS recruitment_run_criteria_id_fkey;
ALTER TABLE recruitment_run ADD CONSTRAINT recruitment_run_criteria_id_fkey
  FOREIGN KEY (criteria_id) REFERENCES recruitment_criteria (criteria_id) ON DELETE RESTRICT;

-- Partial index for current-member queries
CREATE INDEX IF NOT EXISTS idx_club_member_current
  ON club_member (club_id) WHERE until IS NULL;

-- Narrow integer columns to SMALLINT where appropriate
ALTER TABLE club_match ALTER COLUMN boards TYPE SMALLINT;
ALTER TABLE club_match ALTER COLUMN team1_score_x2 TYPE SMALLINT;
ALTER TABLE club_match ALTER COLUMN team2_score_x2 TYPE SMALLINT;
ALTER TABLE club_match_board ALTER COLUMN board TYPE SMALLINT;
ALTER TABLE unresolved_board_player ALTER COLUMN board TYPE SMALLINT;
ALTER TABLE job_schedule ALTER COLUMN interval_hours TYPE SMALLINT;
