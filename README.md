# CCAS

Chess Club Admin System -- a Scala 3 / ZIO application that integrates with the [Chess.com public API](https://www.chess.com/news/view/published-data-api) for club membership tracking, player recruitment, and match history analysis.

## Tech Stack

- Scala 3.8.2, SBT 1.12.8
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
sbt run

# Run tests (requires ccas_test database from docker compose)
sbt test
```

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
| `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_PORT`, `DB_HOST`, `DB_SCHEMA` | PostgreSQL connection (or set `DATABASE_URL` instead) |

Optional overrides with defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | 8080 | HTTP server port |
| `SCHEDULER_POLL_MINUTES` | 5 | How often the scheduler checks for due jobs |
| `CHESS_COM_API_PERMITS` | 16 | Max parallel Chess.com API requests |
| `CHESS_COM_API_COOLDOWN_SECONDS` | 30 | Backoff cooldown after rate limiting |

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
