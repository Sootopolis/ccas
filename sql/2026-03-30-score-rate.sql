BEGIN;

-- 1. recruitment_criteria: add columns in the right position by recreating the table

-- Drop inbound FKs
ALTER TABLE recruitment_run   DROP CONSTRAINT recruitment_run_criteria_id_fkey;
ALTER TABLE recruitment_alias DROP CONSTRAINT recruitment_alias_criteria_id_fkey;

-- Rename old table
ALTER TABLE recruitment_criteria RENAME TO recruitment_criteria_old;

-- Create new table with correct column order
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

-- Copy data (new columns default to NULL)
INSERT INTO recruitment_criteria (
  criteria_id, min_days_since_registration, days_since_last_invited, days_since_rejected,
  nationality_exclude, nationality_countries, exclude_clubs, max_clubs,
  exclude_source_admins, exclude_former_members,
  daily_min_elo, daily_max_elo,
  daily_min_games_finished, daily_min_tm_games_finished,
  daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
  daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches
)
SELECT
  criteria_id, min_days_since_registration, days_since_last_invited, days_since_rejected,
  nationality_exclude, nationality_countries, exclude_clubs, max_clubs,
  exclude_source_admins, exclude_former_members,
  daily_min_elo, daily_max_elo,
  daily_min_games_finished, daily_min_tm_games_finished,
  daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
  daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches
FROM recruitment_criteria_old;

-- Advance the new sequence past existing IDs
SELECT setval('recruitment_criteria_criteria_id_seq',
              COALESCE((SELECT MAX(criteria_id) FROM recruitment_criteria), 0));

DROP TABLE recruitment_criteria_old;

-- Restore inbound FKs
ALTER TABLE recruitment_run
  ADD CONSTRAINT recruitment_run_criteria_id_fkey
  FOREIGN KEY (criteria_id) REFERENCES recruitment_criteria (criteria_id) ON DELETE RESTRICT;

ALTER TABLE recruitment_alias
  ADD CONSTRAINT recruitment_alias_criteria_id_fkey
  FOREIGN KEY (criteria_id) REFERENCES recruitment_criteria (criteria_id) ON DELETE RESTRICT;

-- 2. player_recruitment_cache: add column in the right position by recreating the table

ALTER TABLE player_recruitment_cache RENAME TO player_recruitment_cache_old;

CREATE TABLE player_recruitment_cache (
  player_id              BIGINT PRIMARY KEY REFERENCES player (player_id) ON DELETE RESTRICT,
  fetched_at             TIMESTAMPTZ NOT NULL,
  daily_elo              INT,
  daily_score_rate       DOUBLE PRECISION,
  daily_timeout_pct      DOUBLE PRECISION,
  daily_games_finished   INT,
  club_count             INT,
  ongoing_games          INT,
  ongoing_team_matches   INT,
  tm_games_finished_90d  INT,
  tm_timeout_pct_90d     DOUBLE PRECISION,
  last_daily_timeout_at  TIMESTAMPTZ,
  last_tm_timeout_at     TIMESTAMPTZ
);

INSERT INTO player_recruitment_cache (
  player_id, fetched_at, daily_elo, daily_timeout_pct, daily_games_finished,
  club_count, ongoing_games, ongoing_team_matches, tm_games_finished_90d, tm_timeout_pct_90d,
  last_daily_timeout_at, last_tm_timeout_at
)
SELECT
  player_id, fetched_at, daily_elo, daily_timeout_pct, daily_games_finished,
  club_count, ongoing_games, ongoing_team_matches, tm_games_finished_90d, tm_timeout_pct_90d,
  last_daily_timeout_at, last_tm_timeout_at
FROM player_recruitment_cache_old;

DROP TABLE player_recruitment_cache_old;

COMMIT;
