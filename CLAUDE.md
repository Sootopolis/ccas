# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CCAS (Chess Club Admin System) is a Scala 3 application that pulls data from the Chess.com public API and stores it in a PostgreSQL database for chess club management tasks: tracking membership, analysing member performance, and scouting/recruiting players from other clubs. It includes a backend HTTP server with job scheduling for running these tasks.

## Build & Test Commands

This is an SBT project (Scala 3.8.3, SBT 1.12.8).

- **Compile:** `sbt compile`
- **Run all tests:** `sbt test`
- **Run a single test suite:** `sbt "testOnly ccas.api.TestApiJsonParsing"` (fully qualified object name)
- **Continuous compile on change:** `sbt ~compile`
- **Interactive SBT shell:** `sbt` then run commands without the `sbt` prefix

Tests use ZIO Test (`ZIOSpecDefault`). SQL tests require a running PostgreSQL instance with a `ccas_test` database (see `src/test/resources/application.conf`).

## Architecture

### Layers

The codebase has four main packages:

1. **`ccas.api`** — Chess.com API models and client. Case classes model API JSON responses (e.g., `ApiPlayer`, `ApiClub`, `ApiDailyMatch`). Each API model companion object extends `JsonDecoding[T]` to provide a ZIO JSON decoder. API models are read-only data transfer objects; they are never written to the database directly.

2. **`ccas.analysis`** — Domain tables and business logic. `analysis.tables` contains database-persisted entities (`Player`, `PlayerSnapshot`, `Club`, `ClubMember`, `ClubMatch`, `ClubMatchBoard`, plus recruitment-related tables and history crawl tables like `HistoryMemberQuery`, `HistoryPendingMatch`, `HistoryRun`). `analysis.apps` contains runnable applications (`MembershipApp`, `RecruitmentApp`, `MatchRefApp`, `BlacklistApp`, `HistoryApp`).

3. **`ccas.server`** — Backend HTTP server with job execution and scheduling. `server.jobs` has `JobRunner` (async job execution via forked fibers), `JobRun`/`JobSchedule` (database entities). `server.routes` has zio-http route handlers for jobs, schedules, and health checks. `server.scheduler` has `JobScheduler` (polling-based scheduled job execution). Entry point is `CcasServer extends ZIOAppDefault`.

4. **`ccas.utils`** — Shared infrastructure: HTTP client (`client/`), JSON traits (`json/`), SQL helpers (`sql/`), opaque type utilities (`opaque/`), and pretty-printing (`prettyprinting/`).

### Key Patterns

**Table entity pattern with Magnum:** Each database entity (e.g., `Club`, `Player`) follows a consistent structure:
- A case class annotated with `@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)` and `derives DbCodec`
- Its companion object provides static methods: `createTable`, `selectAll`, `selectId`, `insert`, `insertBatch`, `update`, `upsert`, etc.
- All SQL operations wrap Magnum queries in `connectZIO` (reads) or `transactZIO` (writes/batches)
- Complex queries use `SqlLiteral` for reusable column lists and raw SQL interpolation (`sql"""..."""`)
- Some entities also use Magnum's `Repo[T, T, ID]` or `ImmutableRepo[T, ID]` for standard CRUD

**Type safety with opaque types:** Domain IDs and constrained values use Scala 3 opaque types with companion traits (`StringCompanion`, `StringKeyCompanion`, `IntCompanion`, `LongCompanion`, `DoubleCompanion`) defined in `ccas.utils.opaque`. Each companion provides `JsonCodec`, `DbCodec`, and `DeriveConfig` instances, plus optional validation via `validateRaw` and normalization via `normalize`. `StringKeyCompanion` extends `StringCompanion` with `JsonFieldEncoder`/`JsonFieldDecoder` for types used as JSON object keys. Concrete types are defined in `ccas.api.misc.subtypes` and include: `PlayerId`, `ClubId`, `Username`, `ClubUrlName`, `Elo`, `Percentage`, etc.

**Enum serialization:** Enums extend `EnumJson[T]` for JSON (snake_case wire format, PascalCase in Scala) and/or `EnumSql[T]` for database persistence. Both provide codec instances via `given`. Some enums override `jsonToEnum` for non-standard Chess.com API mappings (e.g., `"closed:fair_play_violations"` → `Fairplay`).

**JSON decoding trait:** API model companions extend `JsonDecoding[T]`, which wraps `JsonDecoder` with convenience methods (`decodeZIO`, string extensions). The derived decoder is provided via `jsonDecoderDerived`. API models use `@jsonMemberNames(SnakeCase)` for field mapping. Global decoders for `Instant` and `URL` are exported automatically.

**ZIO effect types:** SQL operations use `SqlTask[A]` (alias for `IO[SQLException, A]`). Helper functions `connectZIO` and `transactZIO` (in `ccas.utils.sql.SqlZioTypes`) provide the bridge between Magnum's context-function-based API and ZIO effects, returning `ZIO[Transactor, SQLException, A]`. For multi-statement atomic operations, `withTransaction` wraps multiple `connectZIO` calls in a single JDBC transaction (commits on success, rolls back on any failure or interruption) by sharing a proxied connection via a scoped `Transactor`.

### HTTP Client

`ChessComClient` wraps `zio-http` `Client` for making GET requests with automatic JSON decoding. Features include:
- Gate-based admission control with configurable concurrency limit (default 8 permits)
- Adaptive rate limiting: EMA-based request spacing, failure-window throttle-down on 429/403 responses, immediate hard throttle on Cloudflare challenges
- Per-attempt failure recording in `api_fetch_failure` with deduplicated response bodies in `api_response_body`
- Separate retry schedules for 429 (exponential backoff), Cloudflare 403 (fixed delay), normal 403 (single retry), and connection errors (exponential backoff)
- Requires `CCAS_CONTACT_EMAIL` environment variable for the `User-Agent` header
- Batch fetching via `getAll[T](urls)` using `ZIO.foreachPar`

### Backend Server

`CcasServer` (`ccas.server`) is a zio-http server that exposes REST endpoints and runs background jobs:
- **Routes:** `HealthRoutes` (health/readiness), `JobRoutes` (submit and query jobs), `ScheduleRoutes` (CRUD for scheduled jobs). Route handlers use inline JSON codecs for request/response types and map domain exceptions to HTTP status codes (e.g., `JobConflictException` → 409).
- **JobRunner:** Trait-based (`JobRunnerLive`) async executor that forks fibers per job, tracks state in `JobRun` table (ULID IDs via `ulid-creator`), and auto-submits follow-up jobs (e.g., MatchRef after Recruitment, Membership, or History completes). Marks orphaned running jobs as failed on startup. The `submit` method takes an `Option[String] => RIO[..., Any]` effect function, passing the generated job run ID so analysis apps can link their run records back via `job_run_id`.
- **JobScheduler:** Polling daemon (configurable interval) that checks enabled `JobSchedule` entries and submits due jobs to the `JobRunner`.
- **ServerTables:** Ensures both analysis and server database tables exist on startup.

### Database

Uses Magnum (`com.augustnagro.magnum`) for SQL access with PostgreSQL. `DataSourceLayer` reads config from `application.conf` under the `database` prefix and provides a `Transactor` ZLayer. Custom `DbCodec` instances handle `Instant` (via `TIMESTAMPTZ`), `URL`, and `List[String]` (PostgreSQL arrays). Table names are derived from case class names via `CamelToSnakeCase` naming strategy. Server tables (`JobRun`, `JobSchedule`) reference clubs by `club_id` FK; route handlers resolve the slug from HTTP requests to a `ClubId` before submitting jobs. Analysis run tables (`MembershipRun`, `RecruitmentRun`, `HistoryRun`) have an optional `job_run_id` column linking back to the server-level job. Schema migrations for existing databases are managed via manual SQL scripts in the `sql/` directory.

### Test Data

API JSON test fixtures live in `data/test/api/*.json`. Tests in `TestApiJsonParsing` validate that these parse correctly into API model types.
