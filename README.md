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
ccas jobs
ccas membership <club-slug>
ccas recruit --target 30 <club-slug>
ccas blacklist list <club-slug>
ccas --help              # full command tree
ccas <command> --help    # per-command flags
```

Commands: `serve`, `membership`, `history`, `recruit`, `stats`, `jobs`, `logs`, `blacklist {add|list|remove}`, `schedule {list|add|remove}`. A global `--server <url>` overrides the default `http://127.0.0.1:8080`.

`ccas --version` prints the version; `ccas --help` and `ccas <command> --help` show usage.

> **Two gotchas.** The parser (zio-cli) expects options **before** positional arguments — `ccas membership --no-trust-usernames team-alpha`, not `… team-alpha --no-trust-usernames` (a misplaced flag is silently dropped). And the staged binary reads configuration from the **process environment only** — it does not load `.env` (that is auto-sourced for `sbt run` / `sbt runMain`).

### Shell completion (bash)

Install the generated completion script once:

```bash
mkdir -p ~/.local/share/bash-completion/completions
ccas --shell-completion-script "$(command -v ccas)" --shell-type bash 2>/dev/null \
  | sed 's#"${CMDLINE\[@\]}")#"${CMDLINE[@]}" 2>/dev/null)#' \
  > ~/.local/share/bash-completion/completions/ccas
```

`bash-completion` auto-loads it on first use (or `source` it from `~/.bashrc`). `zsh` / `fish` are supported via `--shell-type zsh|fish`. The `2>/dev/null` / `sed` keep a JVM deprecation warning out of the completion output.

Caveat: completion calls back into the binary, so each `<TAB>` boots the JVM (~1s latency). A fast, cache-based completion is tracked in #49.

## Applications

Jobs can be submitted via the REST API or run on a schedule.

| Job | Description |
|-----|-------------|
| **Recruitment** | Scouts players from source clubs, filters by rating/activity/experience criteria, produces recruitment candidates |
| **Membership** | Reconciles the club member list against the Chess.com API, tracks joins and departures |
| **History** | Crawls match archives for club members, discovering new players wave by wave |
| **MatchRef** | Resolves player/club references to concrete match boards (runs automatically after other jobs) |
| **Blacklist** | Adds a player to the recruitment blacklist |

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

All environment variables are listed in [`.env.example`](.env.example). Required variables:

| Variable | Description |
|----------|-------------|
| `CCAS_CONTACT_EMAIL` | Used in `User-Agent` header for Chess.com API requests |
| `DATABASE_URL` *or* `DB_USER` + `DB_PASSWORD` + `DB_NAME` + `DB_PORT` + `DB_HOST` + `DB_SCHEMA` | PostgreSQL connection. `DATABASE_URL` (JDBC form, single-quoted to escape `&`) takes priority; the `DB_*` fields are only consulted when it's absent |

Optional overrides with defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | 8080 | HTTP server port |
| `SCHEDULER_POLL_MINUTES` | 15 | How often the scheduler checks for due jobs. Keep ≥ 15 against Neon so polls don't keep the compute always-warm (it auto-suspends after ~5 min idle, budget is 192 active-hr/mo on free tier) |
| `DB_POOL_MAX` / `DB_POOL_MIN_IDLE` | 20 / 2 | HikariCP pool sizing (set `MIN_IDLE=0` for Neon scale-to-zero) |
| `DB_POOL_CONNECTION_TIMEOUT` / `DB_POOL_IDLE_TIMEOUT` / `DB_POOL_MAX_LIFETIME` / `DB_POOL_KEEPALIVE_TIME` | 30 000 / 600 000 / 1 800 000 / 120 000 ms | HikariCP timeouts |
| `CHESS_COM_API_PERMITS` | 16 | Max parallel Chess.com API requests |
| `CHESS_COM_API_COOLDOWN_SECONDS` | 30 | Backoff cooldown after rate limiting |
| `CHESS_COM_API_CACHE_RETENTION_DAYS` | 7 | How long cached Chess.com responses are kept before startup pruning |

See [`application.conf`](src/main/resources/application.conf) for the full set of tunable parameters.

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
