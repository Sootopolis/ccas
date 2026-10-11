-- #302, ADR 0021: a membership's ends become windows, and when each club's roster was seen gets a table of its own.
-- Idempotent, so safe to run twice. The constraints come in a second transaction: an index built in the transaction
-- that corrected a row still reads the row as it was, and `tstzrange` refuses one that ends before it starts.
--
-- Order, against one database: stop the server (an older binary closes rows without `last_seen`, which the new
-- constraint refuses), run this script, re-stage the launcher, boot.
--
-- Run: psql -v ON_ERROR_STOP=1 -f sql/2026-10-10-membership-ends-are-windows.sql <database>
--
-- Rolling back to an older binary afterwards: drop both constraints, and
-- UPDATE membership_run SET trigger = 'Cli' WHERE trigger = 'Dependency' (it cannot decode the new trigger).

BEGIN;

ALTER TABLE club_member ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS club_roster_coverage (
  club_id           BIGINT PRIMARY KEY,
  first_observed_at TIMESTAMPTZ NOT NULL,
  last_observed_at  TIMESTAMPTZ NOT NULL,
  CHECK (first_observed_at <= last_observed_at),
  FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT
);

-- Each completed run saw the club's roster, a few seconds before it completed.
INSERT INTO club_roster_coverage (club_id, first_observed_at, last_observed_at)
SELECT club_id, min(completed_at), max(completed_at) FROM membership_run
WHERE completed_at IS NOT NULL
GROUP BY club_id
ON CONFLICT (club_id) DO UPDATE SET
  first_observed_at = LEAST(club_roster_coverage.first_observed_at, EXCLUDED.first_observed_at),
  last_observed_at  = GREATEST(club_roster_coverage.last_observed_at, EXCLUDED.last_observed_at);

-- A closed account's last visit can precede the start of the row it ended. The row ends where it starts instead.
UPDATE club_member SET until = since WHERE until < since;

-- An ended roster row was last known in at the latest run before it ended, which listed it, or at its start. No roster
-- ever listed a guessed row, so it was known in only at its start.
UPDATE club_member cm SET last_seen =
  CASE WHEN cm.since_approximate THEN cm.since
  ELSE GREATEST(cm.since, COALESCE(
    (SELECT max(r.completed_at) FROM membership_run r
     WHERE r.club_id = cm.club_id AND r.completed_at IS NOT NULL AND r.completed_at < cm.until),
    cm.since))
  END
WHERE cm.until IS NOT NULL AND cm.last_seen IS NULL;

COMMIT;

-- A session holding a snapshot from before that commit keeps the old rows in the index build, which then stops at
-- "range lower bound must be less than or equal to range upper bound": run the script again once that session ends.
BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'club_member_window') THEN
    ALTER TABLE club_member ADD CONSTRAINT club_member_window
      CHECK ((until IS NULL) = (last_seen IS NULL) AND since <= last_seen AND last_seen <= until);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'club_member_no_overlap') THEN
    ALTER TABLE club_member ADD CONSTRAINT club_member_no_overlap
      EXCLUDE USING gist (club_id WITH =, player_id WITH =, tstzrange(since, until) WITH &&);
  END IF;
END
$$;

COMMIT;
