# Plan: Migrate to Neon Serverless PostgreSQL

## Context

The app currently runs against a local PostgreSQL instance. The goal is to switch to the existing Neon project (CCAS, `sweet-dream-48443299`, `aws-eu-west-2`) as the primary database, with HikariCP pool settings tuned so that Neon compute suspends between job runs and stays within the free-tier compute budget (~192 active hours/month).

The current storage usage on Neon is ~31 MB of a 512 MB limit. The app's transient-error retry logic and `initializationFailTimeout = -1` already handle cold starts gracefully — the only changes needed are configuration.

## Changes

### 1. Add env var overrides for hardcoded pool settings

**File:** `src/main/resources/application.conf` (lines 63-65)

Three pool settings are currently hardcoded with no env var override. Add overrides following the existing pattern:

```hocon
connectionTimeout = 30000
connectionTimeout = ${?DB_POOL_CONNECTION_TIMEOUT}
idleTimeout       = 600000
idleTimeout       = ${?DB_POOL_IDLE_TIMEOUT}
maxLifetime       = 1800000
maxLifetime       = ${?DB_POOL_MAX_LIFETIME}
```

### 2. Update `.env` for Neon

**File:** `.env`

Add `DATABASE_URL` with the Neon connection string (SSL required) and pool tuning overrides. Keep the existing `DB_*` fields — they're mandatory for typesafe config substitution resolution even when `DATABASE_URL` takes priority at runtime.

```env
# Neon
DATABASE_URL=jdbc:postgresql://ep-xxx.eu-west-2.aws.neon.tech/ccas?user=...&password=...&sslmode=require

# Pool tuning for serverless (Neon)
DB_POOL_MIN_IDLE=0
DB_POOL_IDLE_TIMEOUT=30000
DB_POOL_KEEPALIVE_TIME=0

# Tighter cache retention for Neon's 512 MB storage cap
CHESS_COM_API_CACHE_RETENTION_DAYS=3
```

- `MIN_IDLE=0` — no idle connections kept; Neon suspends when all connections close
- `IDLE_TIMEOUT=30000` (30s) — connections close quickly after use, shorter than the 2-min keepalive so keepalive never fires
- `KEEPALIVE_TIME=0` — explicitly disabled (moot with 30s idle timeout, but clearer intent)
- `CHESS_COM_API_CACHE_RETENTION_DAYS=3` — overrides the 7-day default in `application.conf`. Combined with the empty-cache start, keeps `api_response_cache` + `api_response_body` well under 512 MB. Adjust upward if conditional-GET hit rate drops too far.

### 3. Update `.env.example`

**File:** `.env.example`

Document the new env vars in the optional overrides section:

```env
# DB_POOL_CONNECTION_TIMEOUT=30000
# DB_POOL_IDLE_TIMEOUT=600000
# DB_POOL_MAX_LIFETIME=1800000
# DB_POOL_KEEPALIVE_TIME=120000
```

### 4. Purge stale Neon tables and migrate local data

Neon currently has three tables from an older schema (`club`, `player`, `player_snapshot`) that need to be dropped first. Then dump the local DB (schema + data) and restore it to Neon.

The local `api_response_cache` + `api_response_body` + `api_fetch_failure` tables exceed the Neon free-tier 512 MB storage limit on their own (~600 MB locally). They are excluded from the dump — the cache is rebuildable from the network on demand and `Tables.ensureTables` recreates the tables empty on first server start. `api_fetch_failure` is rebuildable diagnostic data and is FK-tied to `api_response_body`, so all three are excluded together.

**Trade-off:** the first job runs against Neon hit the network for every URL until the cache warms (no Fresh / Revalidated hits). One-time cost.

**Step 1 — Drop stale tables on Neon** (via `mcp__neon__run_sql`):

```sql
DROP TABLE IF EXISTS player_snapshot, player, club CASCADE;
```

**Step 2 — Dump the local DB** using credentials from the current `.env`, excluding the cache and failure tables:

```bash
pg_dump -h localhost -p 5432 -U ccas -d ccas -Fc \
  -T api_response_cache \
  -T api_response_body \
  -T api_fetch_failure \
  -f /tmp/ccas.dump
```

**Step 3 — Get the Neon libpq connection string** via `mcp__neon__get_connection_string` (note: libpq format `postgres://...`, not the JDBC URL form used in `DATABASE_URL`).

**Step 4 — Restore to Neon:**

```bash
pg_restore -d "postgres://user:pass@ep-xxx.eu-west-2.aws.neon.tech/ccas?sslmode=require" --no-owner --no-privileges /tmp/ccas.dump
```

`--no-owner`/`--no-privileges` avoids trying to set ownership to the local `ccas` role on Neon.

After this, all migrated tables and data exist on Neon. The three excluded tables are created empty by `ensureTables` on first server start; every other table no-ops since it already exists.

## Files modified

- `src/main/resources/application.conf` — add 3 env var override lines for the pool; lower `chess-com-client.cache.retention-days` default from 30 to 7
- `.env` — add `DATABASE_URL`, pool tuning vars, and `CHESS_COM_API_CACHE_RETENTION_DAYS=3` for the Neon profile
- `.env.example` — document new overrides; bump the documented `CHESS_COM_API_CACHE_RETENTION_DAYS` default from 30 to 7
- `sql/2026-04-28-api-response-body-fk-indexes.sql` — index FK columns referencing `api_response_body` so `deleteOrphans` and `ON DELETE RESTRICT` enforcement stop sequential-scanning the cache and failure tables
- `src/main/scala/ccas/analysis/tables/ApiResponseCache.scala`, `ApiFetchFailure.scala` — mirror the new `CREATE INDEX IF NOT EXISTS` calls in `createTable` so fresh databases pick them up via `Tables.ensureTables`

The `pg_dump` invocation excludes `api_response_cache`, `api_response_body`, and `api_fetch_failure`. They are recreated empty by `ensureTables` on first server start.

No other Scala code changes required.

## Verification

1. `sbt compile` — confirm config parses
2. Run the server briefly and confirm it connects to Neon
3. Confirm cache tables exist empty post-restore: `SELECT count(*) FROM api_response_cache`, `SELECT count(*) FROM api_response_body`, `SELECT count(*) FROM api_fetch_failure` should all return 0
4. Confirm the FK indexes were created on Neon: `\di idx_api_response_cache_body_id` and `\di idx_api_fetch_failure_response_body_id`
5. Check Neon console to verify compute suspends after the server stops or goes idle
6. After ~1 week of normal job activity against Neon, check `pg_size_pretty(pg_total_relation_size('api_response_body'))` — expect well under 100 MB given the 3-day retention. Escalate to per-pattern retention (Step 2b in the cache-shrink plan) if it exceeds ~200 MB.
7. `sbt test` — tests use separate `test/resources/application.conf` pointing at localhost, should be unaffected
