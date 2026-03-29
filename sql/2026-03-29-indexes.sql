-- Add missing indexes identified during schema review.
-- Partial indexes for hot-path queries with constant predicates;
-- composite indexes to avoid sorts on ORDER BY ... DESC patterns;
-- simple indexes for unindexed FK columns and append-only tables.

-- Partial: selectRunning filters kind+club_id within running jobs only (0-3 rows at any time)
CREATE INDEX IF NOT EXISTS idx_job_run_running
  ON job_run (kind, club_id) WHERE status = 'Running';

-- selectRecent: ORDER BY started_at DESC LIMIT n
CREATE INDEX IF NOT EXISTS idx_job_run_started_at
  ON job_run (started_at DESC);

-- Partial: selectClubBatch / countNew always filter status = 'New' (work queue hot set)
CREATE INDEX IF NOT EXISTS idx_history_pending_new
  ON history_pending_match (club_id) WHERE status = 'New';

-- selectLatestInvited, selectLatestRejectedByAlias, selectDeferredByClub
-- all filter player_id + outcome and sort by evaluated_at DESC
CREATE INDEX IF NOT EXISTS idx_rc_player_outcome_eval
  ON recruitment_candidate (player_id, outcome, evaluated_at DESC);

-- FK backref: history_run references club(club_id) but had no index (unlike membership_run, recruitment_run)
CREATE INDEX IF NOT EXISTS idx_history_run_club_id
  ON history_run (club_id);

-- selectLatest: WHERE club_id = ? ORDER BY started_at DESC — upgrade from single-column club_id index
DROP INDEX IF EXISTS idx_recruitment_run_club_id;
CREATE INDEX IF NOT EXISTS idx_recruitment_run_club_started
  ON recruitment_run (club_id, started_at DESC);

-- selectLatest: WHERE club_id = ? ORDER BY started_at DESC — upgrade from single-column club_id index
DROP INDEX IF EXISTS idx_membership_run_club_id;
CREATE INDEX IF NOT EXISTS idx_membership_run_club_started
  ON membership_run (club_id, started_at DESC);

-- selectRecent: WHERE occurred_at >= ? on append-only table with no PK or indexes
CREATE INDEX IF NOT EXISTS idx_api_fetch_failure_occurred_at
  ON api_fetch_failure (occurred_at);
