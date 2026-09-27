-- #254 (ADR 0016): a club or player is its id, and a name is an observation with a window. The statements every
-- database needs by hand, for the whole migration, in one transaction. Idempotent, so safe to run twice.
--
-- The rest runs at boot rather than here: `Tables.ensureTablesOnInit` creates `club_name` and `player_name` and
-- backfills them. Do not replay the issue body's step-5 backfill SQL; it fails on Neon (#254 comment 5816746348).
--
-- Before step 5b's half:
--   1. The database has booted a 5a build (#278, before step 5b's own change). That boot seeded `player_name` from
--      the snapshot history; a 5b boot opens only current names, so it does not qualify. The checks below refuse to
--      run without `player_name`, or when a name `player_snapshot` holds is missing from it.
--   2. No server is running against it: an older binary still writes `player_snapshot.username`, and a 5b binary
--      cannot write a snapshot until the column is gone. Stop the server, run this, re-stage the launcher, boot.
--
-- Run: psql -v ON_ERROR_STOP=1 -f sql/2026-09-26-player-and-club-names.sql <database>

BEGIN;

-- Step 1 (#266): the name tables' exclusion constraints need it, and boot refuses to install it.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Step 3b (#273): `club.slug` is a display cache; `club_name` says who holds a name.
DROP INDEX IF EXISTS club_slug_key;

-- Step 5b: the same for `player.username`. Refuse, rolling the whole transaction back, if 5a has not booted here or
-- if a name `player_snapshot` holds is missing from `player_name` — dropping the column would lose it.
DO $$
DECLARE
  missing bigint;
BEGIN
  IF to_regclass('player_name') IS NULL THEN
    RAISE EXCEPTION 'player_name is missing: boot a 5a build (#278, before step 5b) against this database first';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = 'player_snapshot' AND column_name = 'username') THEN
    SELECT count(*) INTO missing FROM player_snapshot s
    WHERE s.username !~ '^_stale_[0-9]+$'
      AND NOT EXISTS (SELECT 1 FROM player_name n WHERE n.player_id = s.player_id AND n.username = s.username);
    IF missing > 0 THEN
      RAISE EXCEPTION '% player_snapshot rows name a username player_name does not hold for that player', missing;
    END IF;
  END IF;
END $$;

-- Before the UPDATE below: it gives a tombstoned player a name another row stores, which the deferred constraint
-- would queue a check for, and a table with pending checks cannot be altered in the same transaction.
ALTER TABLE player DROP CONSTRAINT IF EXISTS player_username_unique;

-- A tombstoned player's display cache takes back the last name it held, as a club keeps its slug. No window opens,
-- so it still holds no name. Neon's copy of 2026-09-24 had one: 27968256, last `emilie_hegland`, held by 477947340.
UPDATE player p SET username = last.username
FROM (SELECT DISTINCT ON (player_id) player_id, username FROM player_name ORDER BY player_id, since DESC) last
WHERE last.player_id = p.player_id AND p.username ~ '^_stale_[0-9]+$';

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM player WHERE username ~ '^_stale_[0-9]+$') THEN
    RAISE EXCEPTION 'a tombstoned player has no name in player_name to take back';
  END IF;
END $$;

DROP INDEX IF EXISTS idx_player_snapshot_username;
ALTER TABLE player_snapshot DROP COLUMN IF EXISTS username;

COMMIT;
