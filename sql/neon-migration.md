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
```

- `MIN_IDLE=0` — no idle connections kept; Neon suspends when all connections close
- `IDLE_TIMEOUT=30000` (30s) — connections close quickly after use, shorter than the 2-min keepalive so keepalive never fires
- `KEEPALIVE_TIME=0` — explicitly disabled (moot with 30s idle timeout, but clearer intent)

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

**Step 1 — Drop stale tables on Neon** (via `mcp__neon__run_sql`):

```sql
DROP TABLE IF EXISTS player_snapshot, player, club CASCADE;
```

**Step 2 — Dump the local DB** using credentials from the current `.env`:

```bash
pg_dump -h localhost -p 5432 -U ccas -d ccas -Fc -f /tmp/ccas.dump
```

**Step 3 — Get the Neon libpq connection string** via `mcp__neon__get_connection_string` (note: libpq format `postgres://...`, not the JDBC URL form used in `DATABASE_URL`).

**Step 4 — Restore to Neon:**

```bash
pg_restore -d "postgres://user:pass@ep-xxx.eu-west-2.aws.neon.tech/ccas?sslmode=require" --no-owner --no-privileges /tmp/ccas.dump
```

`--no-owner`/`--no-privileges` avoids trying to set ownership to the local `ccas` role on Neon.

After this, all current tables and data exist on Neon. When the server starts, `ensureTables` will no-op since every table already exists.

## Files modified

- `src/main/resources/application.conf` — add 3 env var override lines
- `.env` — add `DATABASE_URL` and pool tuning vars
- `.env.example` — document new overrides

No Scala code changes required.

## Verification

1. `sbt compile` — confirm config parses
2. Run the server briefly and confirm it connects to Neon
3. Check Neon console to verify compute suspends after the server stops or goes idle
4. `sbt test` — tests use separate `test/resources/application.conf` pointing at localhost, should be unaffected
