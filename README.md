# CCAS

Chess Club Admin System -- a Scala 3 / ZIO application that integrates with the [Chess.com public API](https://www.chess.com/news/view/published-data-api) for club membership tracking, player recruitment, and match history analysis.

## Tech Stack

- Scala 3.8.3, SBT 1.12.8
- ZIO 2.x (zio-http, zio-json, zio-config)
- Magnum ORM + PostgreSQL 17
- Docker Compose for local dev

## Getting Started

**Prerequisites:** JDK 21+, SBT, Docker

```bash
# Start PostgreSQL (creates both ccas and ccas_test databases)
docker compose up -d

# Configure environment
cp .env.example .env   # edit with your values

# Run the server (default port 8080)
sbt "runMain ccas.cli.Main serve"

# Run tests (requires ccas_test database from docker compose)
sbt test
```

## Install the `ccas` CLI

`sbt stage` (sbt-native-packager) produces two launcher scripts under `target/universal/stage/bin/`:

- `ccas` — the CLI / primary entry point
- `ccas-server` — the standalone server entry (used by hosted deploys)

To run `ccas` from anywhere, stage once and symlink the launcher into a directory on your `PATH`:

```bash
sbt stage
ln -sf "$PWD/target/universal/stage/bin/ccas" ~/.local/bin/ccas   # ensure ~/.local/bin is on your PATH
ccas --help
```

The symlink keeps working across rebuilds — `sbt stage` regenerates the launcher in place. Re-run `sbt stage` after changing code.

## Using the CLI

The CLI is a thin HTTP client to a local `CcasServer`, so start a server first (it is not auto-spawned):

```bash
# Terminal 1 — the server. Needs DATABASE_URL/DB_* and CCAS_CONTACT_EMAIL.
# Under sbt these are sourced from .env automatically:
sbt "runMain ccas.cli.Main serve"     # boots CcasServer on 127.0.0.1:8080
# Or, with those vars exported into the environment, the staged binary:  ccas serve

# Terminal 2 — commands. No environment needed; they just call the server.
ccas use-club team-alpha          # set the current club once…
ccas jobs
ccas membership                   # …then commands target it with no slug
ccas membership --club team-beta  # override for one command
ccas membership --all             # every managed club
ccas recruit --target 30 --club team-alpha
ccas blacklist list --club team-alpha
ccas --help              # full command tree
ccas <command> --help    # per-command flags
```

Commands: `serve`, `stop`, `use`, `membership`, `history`, `recruit`, `stats`, `jobs`, `logs`, `blacklist {add|list|remove}`, `schedule {list|add|remove}`, `club {add|remove|list}`.

**Club targeting.** Slug-requiring commands take the club via `--club <slug>` rather than a positional argument; `membership`/`history` accept a comma-separated `--club a,b` or `--all` (every managed club) — but not both at once (`--all` with `--club` is rejected as a likely mistake). When neither is given, the command falls back to the **current club** set with `ccas use-club <slug>` (stored as `current_club` in the [config file](#cli-config-file)); an explicit `--club`/`--all` always wins. `ccas use-club` is a local config write — no server call — so it works offline and only warns (does not reject) if the slug isn't among the cached clubs.

The server URL resolves in order: a global `--server <url>` flag, else `api_url` from the [config file](#cli-config-file), else the built-in default `http://127.0.0.1:8080`.

`ccas --version` prints the version; `ccas --help` and `ccas <command> --help` show usage.

### Running the server detached

`ccas serve` runs in the foreground (logs to stdout, Ctrl-C to stop). To background it:

```bash
ccas serve --detach   # spawns a detached server, waits for /health/ready, prints "started, pid=<n>"
ccas stop             # reads the pid file, sends SIGTERM, waits for a clean shutdown
```

The detached server writes its pid to `${XDG_STATE_HOME:-~/.local/state}/ccas/ccas.pid` and its stdout/stderr to `<log_dir>/server.log` (`log_dir` from the [config file](#cli-config-file), default `~/.local/state/ccas/logs`). A second `ccas serve --detach` while one is running exits with `already running, pid=<n>`. For a supervised/long-lived server (systemd, Docker, Render) run the foreground `ccas-server` binary instead.

> **Two gotchas.** The parser (zio-cli) expects options **before** positional arguments — `ccas blacklist add --club team-alpha alice bob`, not `… alice bob --club team-alpha` (a misplaced flag is silently swallowed as a username). And the staged binary reads configuration from the **process environment only** — it does not load `.env` (that is auto-sourced for `sbt run` / `sbt runMain`).

### Shell completion

`ccas completion <bash|zsh|fish>` prints a self-contained, **pure-shell** completion script. It boots the JVM once when you install it; afterwards every `<TAB>` runs entirely in the shell (no JVM, no network), so completion is instant. The scripts complete subcommands and flags, plus **club slugs** (after `--club`, and for the `ccas use-club` argument) and **recent job ids** (for `ccas logs`) read from cache files.

```bash
# bash — install once (bash-completion auto-loads it)
ccas completion bash > ~/.local/share/bash-completion/completions/ccas

# zsh — add to ~/.zshrc, after `autoload -Uz compinit && compinit`
eval "$(ccas completion zsh)"

# fish
ccas completion fish > ~/.config/fish/completions/ccas.fish
```

Dynamic candidates come from `${XDG_CACHE_HOME:-~/.cache}/ccas/clubs.txt` and `recent-jobs.txt`, refreshed automatically as you run normal commands (the club list is fetched from `GET /api/clubs` at most every few hours; each submitted job id is recorded). On a fresh install these are empty, so only subcommands and flags complete until the first command populates them — unless `default_clubs` is set in the [config file](#cli-config-file), which seeds the club list so completion works immediately.

The committed `completions/ccas.bash` is the generated output of `ccas completion bash`; `TestCcasCompletion` fails if it drifts from the emitter or if a new subcommand/flag isn't covered. Regenerate it with `ccas completion bash > completions/ccas.bash`.

## Applications

Jobs can be submitted via the REST API or run on a schedule.

| Job             | Description                                                                                                       |
|-----------------|-------------------------------------------------------------------------------------------------------------------|
| **Recruitment** | Scouts players from source clubs, filters by rating/activity/experience criteria, produces recruitment candidates |
| **Membership**  | Reconciles the club member list against the Chess.com API, tracks joins and departures                            |
| **History**     | Crawls match archives for club members, discovering new players wave by wave                                      |
| **MatchRef**    | Resolves player/club references to concrete match boards (runs automatically after other jobs)                    |
| **Blacklist**   | Adds a player to the recruitment blacklist                                                                        |

## API

### Health

```
GET  /health          200 (liveness)
GET  /health/ready    200 | 503 (checks DB connectivity)
```

### Jobs

```
POST /api/jobs/recruitment
     { clubSlug, alias?, target?, cumulative?, sourceClubs?, timeLimitMinutes?, explore? }

POST /api/jobs/membership
     { clubSlug, trustUsernames? }

POST /api/jobs/history
     { clubSlug, full?, refresh? }

POST /api/jobs/matchref
     (no body)

POST /api/jobs/blacklist
     { clubSlug, username, reason?, expiresAt? }

GET  /api/jobs          List recent jobs (last 50)
GET  /api/jobs/:id      Job status by ID
```

### Schedules

```
GET    /api/schedules          List all schedules
POST   /api/schedules          { kind, clubSlug?, params?, intervalHours }
PUT    /api/schedules/:id      { intervalHours?, enabled?, params? }
DELETE /api/schedules/:id
```

## Configuration

All environment variables are listed in [`.env.example`](.env.example). For the packaged binary the easiest way to set them is **`ccas config init`** (see [Server config file](#server-config-file-ccas-config) below), which persists them to a local file the server reads at boot — no hand-exported env vars. Required variables:

| Variable | Description |
|----------|-------------|
| `CCAS_CONTACT_EMAIL` | Used in `User-Agent` header for Chess.com API requests |
| `DATABASE_URL` *or* `DB_USER` + `DB_PASSWORD` + `DB_NAME` + `DB_PORT` + `DB_HOST` + `DB_SCHEMA` | PostgreSQL connection. `DATABASE_URL` (JDBC form, single-quoted to escape `&`) takes priority; the `DB_*` fields are only consulted when it's absent |

Optional overrides with defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | 8080 | HTTP server port |
| `SERVER_HOST` | 127.0.0.1 | Bind address. Loopback by default (single-user, no-auth local model); set `0.0.0.0` for hosted deploys |
| `SCHEDULER_POLL_MINUTES` | 15 | How often the scheduler checks for due jobs. Keep ≥ 15 against Neon so polls don't keep the compute always-warm (it auto-suspends after ~5 min idle, budget is 192 active-hr/mo on free tier) |
| `SCHEDULER_MATCHREF_INTERVAL_HOURS` / `SCHEDULER_MATCHREF_ENABLED` | 24 / true | Boot-seed cadence/enable for the global `MatchRef` (rename-recovery) maintenance schedule. Seed-only: applied on a fresh DB; once the row exists, edit it via `ccas schedule` instead |
| `SCHEDULER_CLUBDATA_INTERVAL_HOURS` / `SCHEDULER_CLUBDATA_ENABLED` | 6 / true | Boot-seed cadence/enable for the global `ClubData` (club/admin refresh) maintenance schedule. Seed-only, same as above |
| `DB_POOL_MAX` / `DB_POOL_MIN_IDLE` | 20 / 2 | HikariCP pool sizing (set `MIN_IDLE=0` for Neon scale-to-zero) |
| `DB_POOL_CONNECTION_TIMEOUT` / `DB_POOL_IDLE_TIMEOUT` / `DB_POOL_MAX_LIFETIME` / `DB_POOL_KEEPALIVE_TIME` | 30 000 / 600 000 / 1 800 000 / 120 000 ms | HikariCP timeouts |
| `CHESS_COM_API_PERMITS` | 16 | Max parallel Chess.com API requests |
| `CHESS_COM_API_COOLDOWN_SECONDS` | 30 | Backoff cooldown after rate limiting |

See [`application.conf`](src/main/resources/application.conf) for the full set of tunable parameters.

### Server config file (`ccas config`)

The packaged `ccas` / `ccas-server` binaries read the settings above from the **process environment** — they do **not** load a `.env` file (that's auto-sourced only for `sbt run`). Rather than export them by hand before every `ccas serve`, manage them with `ccas config`, which persists them to `${XDG_CONFIG_HOME:-~/.config}/ccas/ccas.env` — an owner-only (`0600`) `KEY=VALUE` file:

```
ccas config init                   # interactive wizard: contact email, DB connection, port
ccas config set SERVER_PORT 9090   # set one value
ccas config get DATABASE_URL       # print one value (raw)
ccas config list                   # show known settings (secrets shown as ****)
ccas config list --show-secrets    # reveal secret values
ccas config unset SERVER_PORT      # remove a value
ccas config path                   # print the file's path
```

At boot the server applies this file as a JVM overlay, so the **resolution order is: process environment → `ccas.env` → built-in default.** A real env var (or a systemd `Environment=` / container secret) always wins over the file, so hosted deployments keep injecting secrets through the environment. `DATABASE_URL` and `DB_PASSWORD` live in the `0600` file and are redacted by `list`/`show` unless `--show-secrets`. The file is plain env-var form, so it doubles as a systemd `EnvironmentFile`. `ccas config set DATABASE_URL <url>` stores the URL literally (no shell-quoting needed) — only single-quote the `&` yourself if you intend to `source` the file in a POSIX shell. This is the *server*-bootstrap file; it is separate from the CLI client's [`config.conf`](#cli-config-file).

### DB-owned settings

Some app-wide policy lives in the `app_setting` table (`key TEXT PK, value TEXT`) rather than HOCON/env, so it stays consistent across every process on one DB and is tunable without a redeploy. Each setting has a compiled-in default used when its row is absent; the DB row, once set, overrides it. Today the only key is `cache_retention_days` (default 7) — how long cached Chess.com responses are kept before the startup pruning sweep. Change it with SQL:

```sql
INSERT INTO app_setting (key, value) VALUES ('cache_retention_days', '14')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
```

It takes effect on each process's next startup (the value is read once in `Tables.ensureTables`).

### Deployment model

**One `CcasServer` per database is the supported model.** Running CLI commands alongside that server against the same DB is fine — the CLI has no `JobRunner`, so it never touches scheduled-job state; it only shares the Chess.com egress.

Running **two or more servers** against one DB is **not supported** and is at your own risk: a starting server marks *all* `Running` jobs as failed with no instance-ownership filter (it would kill another server's live jobs — #110), and the Chess.com rate limiter is per-process (N servers ≈ N× the per-IP request rate — #111). Multi-server hosting is tracked separately under #60 and is gated on fixing both.

### CLI config file

The `ccas` CLI (the client, not the server) reads optional settings from `${XDG_CONFIG_HOME:-~/.config}/ccas/config.conf` (HOCON):

```hocon
api_url       = "http://127.0.0.1:8080"        # default server URL
default_clubs = ["team-alpha", "team-beta"]    # seed shell-completion suggestions
log_dir       = "~/.local/state/ccas/logs"     # where `ccas serve --detach` writes server.log
current_club  = "team-alpha"                   # default club when a command omits --club (set via `ccas use-club`)
```

- **Server URL** resolves `--server <url>` flag → `api_url` → built-in `http://127.0.0.1:8080`.
- **`default_clubs`** seeds the completion club list on a fresh install (before any `GET /api/clubs` round-trip); real slugs replace it on the next command.
- **`current_club`** is the club a slug-requiring command targets when neither `--club` nor `--all` is given. Set it with `ccas use-club <slug>` (which rewrites just this key, preserving your other keys and comments) rather than editing by hand.
- A missing file is fine — the CLI falls back to built-in defaults. A malformed file fails fast with `error: invalid config file <path>: …` (exit 2).

This is separate from the server's `application.conf` / environment configuration above.

## Backups

Production data lives on Neon's free plan, which retains only 24h of point-in-time recovery. [`scripts/backup-neon.sh`](scripts/backup-neon.sh) takes a weekly compressed logical dump to local disk as the disaster-recovery floor. It is read-only (`pg_dump` only) and reuses the same connection config as the app — `DATABASE_URL` (JDBC form) takes priority, otherwise the `DB_*` fields. The rebuildable cache / diagnostics tables (`api_response_cache`, `api_response_body`, `api_fetch_failure`) are dumped schema-only (`--exclude-table-data`) to keep dumps small.

| Variable | Default | Description |
|----------|---------|-------------|
| `CCAS_BACKUP_DIR` | `~/ccas-backups` | Output directory for `.dump` files |
| `CCAS_BACKUP_RETAIN` | 6 | Number of most-recent dumps to keep (older are pruned) |

Run it once manually (with the DB env loaded), then schedule weekly. Sunday 04:00 UTC, sourcing the repo `.env` so the same connection config is reused:

```cron
0 4 * * 0 set -a; . /path/to/ccas/.env; set +a; /path/to/ccas/scripts/backup-neon.sh >> ~/ccas-backups/backup.log 2>&1
```

A weekly cadence is one Neon compute wake per week — negligible against the 192 active-hr/mo free-tier budget. Restore a dump with `pg_restore` (use a version **≥** the `pg_dump` that wrote the file — a custom-format archive can't be read by an older `pg_restore`):

```bash
pg_restore --no-owner --no-privileges -d <target-conn> ccas-<stamp>.dump
```

## Project Structure

```
src/main/scala/ccas/
  api/         Chess.com API models and client
  analysis/    Domain tables and business logic (apps)
  server/      HTTP server, job runner, scheduler
  utils/       Shared infrastructure (HTTP client, JSON, SQL, logging)
```

## Further Reading

- [Adaptive rate limiting](docs/adaptive-rate-limiting.md) -- design notes on the Chess.com API throttling strategy
- [CLAUDE.md](CLAUDE.md) -- detailed architecture and code patterns for AI-assisted development
