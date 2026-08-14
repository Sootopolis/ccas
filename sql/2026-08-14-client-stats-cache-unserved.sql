-- #211 / #215 (merged as ea8e7cc) add client_stats.cache_unserved: the reconciling term for the optimistic
-- cache_hits / cache_revalidations, which are incremented on the metadata lookup before CacheableResult's lazy
-- getValue has read anything. Served entries are hits + revalidations - unserved.
--
-- Two statements, for the same reason as 2026-04-25-drop-client-stats-cache-defaults.sql: the DEFAULT is
-- load-bearing only for backfilling the rows that already exist. ClientStats.upsert lists every column explicitly,
-- so it never fires afterwards, and leaving it in place would make raw-SQL inserts look optional about a column that
-- is always written -- as well as making this table disagree with ClientStats.createTable, which declares no
-- defaults on any of the four cache counters.
--
-- Apply before deploying ea8e7cc or later against the database. ClientStatsPersistence swallows flush errors by
-- design (.tapError(logWarning).ignore) so that statistics can never fail a run, which means a missing column
-- surfaces as one WARN per flush interval and no new client_stats rows -- not a crash.

ALTER TABLE client_stats ADD COLUMN cache_unserved BIGINT NOT NULL DEFAULT 0;
ALTER TABLE client_stats ALTER COLUMN cache_unserved DROP DEFAULT;
