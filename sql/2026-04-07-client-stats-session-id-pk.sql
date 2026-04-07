-- Replace surrogate BIGSERIAL id with session_id as primary key
ALTER TABLE client_stats DROP CONSTRAINT client_stats_pkey;
ALTER TABLE client_stats DROP COLUMN id;
ALTER TABLE client_stats ADD PRIMARY KEY (session_id);
ALTER TABLE client_stats DROP CONSTRAINT IF EXISTS client_stats_session_id_key;
