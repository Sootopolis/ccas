-- 1. Add members_count to club
ALTER TABLE club ADD COLUMN IF NOT EXISTS members_count INT;

-- 2. Create club_admin table
CREATE TABLE IF NOT EXISTS club_admin (
  club_id    BIGINT NOT NULL REFERENCES club (club_id) ON DELETE RESTRICT,
  player_id  BIGINT NOT NULL REFERENCES player (player_id) ON DELETE RESTRICT,
  PRIMARY KEY (club_id, player_id)
);

-- 3. Recreate recruitment_criteria with avoid_admin_min_club_size in correct column position
--    (after exclude_source_admins, before exclude_former_members)
--    recruitment_alias and recruitment_run both FK to this table, so we must
--    drop those constraints before the rename and re-add them afterward.

ALTER TABLE recruitment_alias DROP CONSTRAINT IF EXISTS recruitment_alias_criteria_id_fkey;
ALTER TABLE recruitment_run   DROP CONSTRAINT IF EXISTS recruitment_run_criteria_id_fkey;

ALTER TABLE recruitment_criteria RENAME TO recruitment_criteria_old;

CREATE TABLE recruitment_criteria (
  criteria_id                    BIGSERIAL PRIMARY KEY,
  min_days_since_registration    INT,
  days_since_last_invited        INT,
  days_since_rejected            INT,
  nationality_exclude            BOOLEAN NOT NULL,
  nationality_countries          TEXT[] NOT NULL,
  exclude_clubs                  BIGINT[] NOT NULL,
  max_clubs                      INT,
  exclude_source_admins          BOOLEAN NOT NULL,
  avoid_admin_min_club_size      INT,
  exclude_former_members         BOOLEAN NOT NULL,
  daily_min_elo                  INT,
  daily_max_elo                  INT,
  daily_min_score_rate           DOUBLE PRECISION,
  daily_max_score_rate           DOUBLE PRECISION,
  daily_min_games_finished       INT,
  daily_min_tm_games_finished    INT,
  daily_max_timeout_percent      DOUBLE PRECISION,
  daily_max_tm_timeout_percent   DOUBLE PRECISION,
  daily_max_hours_per_move       INT,
  daily_min_ongoing_games        INT,
  daily_max_ongoing_games        INT,
  daily_min_ongoing_team_matches INT
);

INSERT INTO recruitment_criteria (
  criteria_id,
  min_days_since_registration, days_since_last_invited, days_since_rejected,
  nationality_exclude, nationality_countries,
  exclude_clubs, max_clubs, exclude_source_admins, exclude_former_members,
  daily_min_elo, daily_max_elo, daily_min_score_rate, daily_max_score_rate,
  daily_min_games_finished, daily_min_tm_games_finished,
  daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
  daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches
)
SELECT
  criteria_id,
  min_days_since_registration, days_since_last_invited, days_since_rejected,
  nationality_exclude, nationality_countries,
  exclude_clubs, max_clubs, exclude_source_admins, exclude_former_members,
  daily_min_elo, daily_max_elo, daily_min_score_rate, daily_max_score_rate,
  daily_min_games_finished, daily_min_tm_games_finished,
  daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
  daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches
FROM recruitment_criteria_old;

SELECT setval(
  pg_get_serial_sequence('recruitment_criteria', 'criteria_id'),
  (SELECT COALESCE(MAX(criteria_id), 0) FROM recruitment_criteria)
);

DROP TABLE recruitment_criteria_old;

ALTER TABLE recruitment_alias
  ADD CONSTRAINT recruitment_alias_criteria_id_fkey
  FOREIGN KEY (criteria_id) REFERENCES recruitment_criteria (criteria_id) ON DELETE RESTRICT;

ALTER TABLE recruitment_run
  ADD CONSTRAINT recruitment_run_criteria_id_fkey
  FOREIGN KEY (criteria_id) REFERENCES recruitment_criteria (criteria_id) ON DELETE RESTRICT;
