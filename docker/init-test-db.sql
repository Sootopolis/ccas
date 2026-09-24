-- Extensions are per database, and the name tables' exclusion constraints (club_name, player_name) need btree_gist in
-- each one CCAS uses (ADR 0016):
-- first `ccas`, the POSTGRES_DB this script connects to, then `ccas_test`.
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE DATABASE ccas_test;
\connect ccas_test
CREATE EXTENSION IF NOT EXISTS btree_gist;
