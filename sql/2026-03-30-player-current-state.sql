-- Move current player state (username, status, title, since) from player_snapshot to player.
-- After this migration, player_snapshot is purely historical.

ALTER TABLE player ADD COLUMN username TEXT;
ALTER TABLE player ADD COLUMN status TEXT;
ALTER TABLE player ADD COLUMN title TEXT;
ALTER TABLE player ADD COLUMN since TIMESTAMPTZ;

UPDATE player p SET (username, status, title, since) = (
  SELECT ps.username, ps.status, ps.title, ps.since
  FROM player_snapshot ps
  WHERE ps.player_id = p.player_id
  ORDER BY ps.since DESC LIMIT 1
);

ALTER TABLE player ALTER COLUMN username SET NOT NULL;
ALTER TABLE player ALTER COLUMN status SET NOT NULL;
ALTER TABLE player ALTER COLUMN since SET NOT NULL;

ALTER TABLE player ADD CONSTRAINT player_username_unique UNIQUE (username)
  DEFERRABLE INITIALLY DEFERRED;

-- Remove now-redundant latest snapshots (keep only historical ones)
DELETE FROM player_snapshot ps
WHERE ps.since = (
  SELECT MAX(ps2.since) FROM player_snapshot ps2 WHERE ps2.player_id = ps.player_id
);
