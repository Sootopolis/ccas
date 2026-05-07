-- Track how many matches were terminated to status='Aborted' during a HistoryApp run because Chess.com
-- returned the permanent 404 body shape ({"code": 0, "message": "Match \"X\" not found."}). Counterpart to
-- the new 'Aborted' ClubMatchStatus variant; lets us see how many terminal-skip transitions a run produced
-- without scanning club_match.status for every clubId.

ALTER TABLE history_run ADD COLUMN IF NOT EXISTS aborted_matches INT NOT NULL DEFAULT 0;

-- One-off audit query for matches now stuck on Aborted:
--   SELECT match_id, fetched_at FROM club_match WHERE status = 'Aborted' ORDER BY fetched_at DESC;
