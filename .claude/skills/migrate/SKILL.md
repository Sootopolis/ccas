---
name: migrate
description: Apply a schema change or a correction of stored data to CCAS's existing databases — local `ccas` and production — from script through rehearsal, backup and migration to a verified boot. Use when a change edits a `createTable` or corrects stored rows, or when the user asks to migrate a database.
---

# Migrate the databases

Booting the server runs `ServerTables.ensureTables`, which creates missing tables and indexes and runs the backfills written into it, but never alters an existing table's columns or constraints. Those changes, and any one-off correction of stored rows, reach an existing database by hand (CLAUDE.md, Schema changes). Production keeps little history of its own and its backup is a dump on the user's machine, so the steps run in this order and none is skipped without saying so. Steps 1 and 2 can run before the merge; steps 3 to 7 run after it, since the server is down from step 3 until step 7 boots the new build.

Production is on Neon today. Everything specific to Neon is in the last section, so a move to another provider rewrites that section and leaves the steps alone.

## 0. Check the tools, and where commands run

Steps 1, 2, 5 and 6 reach production through its MCP server (see the last section). If its tools are neither loaded nor listed as deferred, say so and offer the setup command.

Anything else aimed at the production server or database, `ccas server status` and `ccas server down` included, the user runs in a terminal opened in the `main` worktree, never through `!`. A connection string must not pass through the chat, and Claude's shell and `!` commands carry the environment the session started with: in the `wip` tree that is direnv's, whose `DATABASE_URL` and `SERVER_PORT` point at the local database and server. `printenv DIRENV_DIR` prints nothing when it is absent.

## 1. Write the script

- **Find what production lacks before writing.** Compare the code's `createTable`s against a read-only query of production (`to_regclass`, `information_schema`, the constraint by name). A new table appears at the next boot, but the script creates it too whenever its own steps need it, such as a backfill or a foreign key.
- **Idempotent, transactional, loud**: `IF NOT EXISTS`, guarded `DO` blocks, `BEGIN`/`COMMIT`, run with `psql -v ON_ERROR_STOP=1`.
- **The header says** the order of steps against one database, and how to roll back to the older binary. `sql/2026-10-10-membership-ends-are-windows.sql` is the model.
- It lives in the scratchpad unless it is intricate enough to be worth replaying; `sql/` is an archive, not a ledger.

## 2. Rehearse

Never let production be the first database to run it.

- **On production's data**: restore the latest production backup in full into a throwaway local database as the `ccas` role (README, Backups), or use a branch of production where the provider offers one. Even pure DDL needs the rows: a new constraint or unique index fails only on the data that breaks it. If production has changed since that backup, ask the user for a fresh one.
- **Run the script twice** there, as in step 5: the second run proves it idempotent.
- **Then, on a local restore, boot it**: run the new code's tables as the next boot will, with `DATABASE_URL=<its URL> sbt --server -batch 'runMain ccas.server.ServerTables'`. An index or backfill that production's rows break fails here instead of at the boot.
- **For DDL, compare** it with a second throwaway database where only the new code ran (`CREATE EXTENSION btree_gist` first, then the same `runMain`), by diffing `pg_dump -s` of the two. Expect only the `\restrict` tokens and column order to differ, since `ADD COLUMN` appends where a `createTable` may not; anything else is a defect in the script.

## 3. Stop the server

First settle which schedules should fire when the new build boots (README, Troubleshooting, "A scheduled job fires late, or not at all"): the API that disables one needs the server running. Then the user, in a terminal opened in the `main` worktree (step 0), runs `ccas server status` and stops the server: `ccas server down` for a detached one, Ctrl-C in its terminal for a foreground one. An older binary may write rows the new schema refuses, and anything it writes after the backup is missing from it.

## 4. Back up

The user takes the production backup (see the last section), in a terminal opened in the `main` worktree (step 0).

## 5. Migrate

Run the script as the role that owns the tables: check `current_user` against `pg_tables.tableowner` first, on each database. Locally that is `ccas` (`psql -U ccas`, or `PGOPTIONS='-c role=ccas'`); run as anyone else, new objects belong to them and the app fails on them. Local `ccas` first, then production, through its MCP on the user's go-ahead to that step. If the MCP is missing or a production action is denied, hand over the alternative in the last section rather than retrying.

## 6. Verify

Query each database for what the script promised: the new tables, columns and constraints, and the count of corrected rows against the rehearsal's.

## 7. Re-stage and boot

Re-stage the launcher and boot, as `/ship` step 6b describes. Which schedules fire was settled at step 3.

## Report

What ran against which database, the verification results, and anything handed over or skipped.

## Production on Neon

- **MCP**: the `mcp__neon__*` tools. Set up with `claude mcp add --scope user --transport http neon https://mcp.neon.tech/mcp`, then authorise it in the browser. Pass `project_id` and `database_name` (`ccas`) on every call, and `branch_id` to work on a branch. Read with `run_sql`. Migrate with `run_sql_transaction`, which takes a list of statements and runs them as one transaction: leave out `BEGIN` and `COMMIT`, keep each `DO` block whole as one statement, and make one call per transaction in the script. It connects as the role that owns the tables; step 5's check confirms it.
- **Without the MCP**: the user runs the script in the Neon console's SQL editor, or with `psql` and the connection string the console gives, in their own terminal. The string never enters the chat.
- **History**: the point-in-time window is in README, Backups; the backup is the real floor.
- **Backup**: `scripts/backup-neon.sh` (README, Backups).
- **Branches**: `create_branch` gives a copy-on-write branch of production to rehearse on. Ask before creating one, and delete it once the rollout is verified.
