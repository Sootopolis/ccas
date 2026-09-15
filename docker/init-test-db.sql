-- Extensions are per database, and club_name's exclusion constraint needs btree_gist in each one CCAS uses (ADR 0016):
-- first `ccas`, the POSTGRES_DB this script connects to, then `ccas_test`.
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE DATABASE ccas_test;
\connect ccas_test
CREATE EXTENSION IF NOT EXISTS btree_gist;
