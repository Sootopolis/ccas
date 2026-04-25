-- The DEFAULT 0 on the cache counters was load-bearing only during the 2026-04-16 backfill;
-- ClientStats.upsert always lists every column explicitly, so the defaults never fire now.
-- Drop them to keep raw-SQL inserts honest about what they're writing.

ALTER TABLE client_stats ALTER COLUMN cache_hits DROP DEFAULT;
ALTER TABLE client_stats ALTER COLUMN cache_revalidations DROP DEFAULT;
ALTER TABLE client_stats ALTER COLUMN cache_misses DROP DEFAULT;
