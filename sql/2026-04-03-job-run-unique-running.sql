-- Replace the non-unique partial index on running jobs with a unique one to prevent
-- phantom-row races where two concurrent job submissions both see no running job.
-- COALESCE handles NULL club_id (e.g. MatchRef jobs) since NULLs are distinct in unique indexes.

DROP INDEX IF EXISTS idx_job_run_running;

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_run_running_unique
ON job_run (kind, COALESCE(club_id, -1)) WHERE status = 'Running';
