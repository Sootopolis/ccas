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

2. **`ccas.analysis`** — Domain tables and business logic. `analysis.tables` contains database-persisted entities: core (`Player`, `PlayerSnapshot`, `Club`, `ClubAdmin`, `ClubMember`, `ClubMatch`, `ClubMatchBoard`, `ClubMatchGame`), ref resolution (`ClubMatchRef`, `PlayerMatchRef`, `PlayerTournamentRef`, `ClubRefSkip`, `PlayerRefSkip`, `UnresolvedBoardPlayer`, `UnresolvedMatchClub`), recruitment (`RecruitmentCriteria`, `RecruitmentAlias`, `RecruitmentBlacklist`, `RecruitmentRun`, `RecruitmentCandidate`, `PlayerRecruitmentCache`), history crawl (`HistoryMemberQuery`, `HistoryPendingMatch`, `HistoryRun`), run tracking (`MembershipRun`), and API diagnostics / caching (`ApiFetchFailure`, `ApiResponseBody`, `ApiResponseCache`, `ClientConfig`, `ClientStats`). `analysis.apps` contains runnable applications (`MembershipApp`, `RecruitmentApp`, `RefApp`, `HistoryApp`, `StatsApp`, `ClubDataApp`) and shared helpers (`PlayerUpdater`). `BlacklistApp` is a CLI-only tool in the recruitment package for managing blacklisted players; it is not a server job. `ClubDataApp` refreshes club profile data (admins, member count, slug, latest match activity) for all known clubs (or a specific subset when invoked with slug arguments on the CLI) and is not exposed as an HTTP route — it runs from CLI or scheduler only, as a data-integrity job.

3. **`ccas.server`** — Backend HTTP server with job execution and scheduling. `server.jobs` has `JobRunner` (async job execution via forked fibers), `JobRun`/`JobSchedule` (database entities). `server.routes` has zio-http route handlers for jobs, schedules, and health checks. `server.scheduler` has `JobScheduler` (polling-based scheduled job execution). Entry point is `CcasServer extends ZIOAppDefault`.

4. **`ccas.utils`** — Shared infrastructure: HTTP client (`client/`), JSON traits (`json/`), SQL client and helpers (`sql/`), opaque type utilities (`opaque/`), and pretty-printing (`prettyprinting/`).

### Key Patterns

**Table entity pattern with Magnum:** Each database entity (e.g., `Club`, `Player`) follows a consistent structure:
- A case class annotated with `@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)` and `derives DbCodec`
- Its companion object provides static methods: `createTable`, `selectAll`, `selectId`, `insert`, `insertBatch`, `update`, `upsert`, etc.
- All SQL operations wrap Magnum queries in `PostgresClient.connectZIO` (reads) or `PostgresClient.transactZIO` (writes/batches)
- Complex queries use `SqlLiteral` for reusable column lists and raw SQL interpolation (`sql"""..."""`)
- Some entities also use Magnum's `Repo[T, T, ID]` or `ImmutableRepo[T, ID]` for standard CRUD

**Type safety with opaque types:** Domain IDs and constrained values use Scala 3 opaque types with companion traits (`StringCompanion`, `StringKeyCompanion`, `IntCompanion`, `LongCompanion`, `DoubleCompanion`) defined in `ccas.utils.opaque`. Each companion provides `JsonCodec`, `DbCodec`, and `DeriveConfig` instances, plus optional validation via `validateRaw` and normalization via `normalize`. `StringKeyCompanion` extends `StringCompanion` with `JsonFieldEncoder`/`JsonFieldDecoder` for types used as JSON object keys. Concrete types live in two places: `ccas.api.misc.subtypes` for Chess.com domain IDs (`PlayerId`, `ClubId`, `Username`, `ClubUrlName`, `Elo`, `Percentage`, etc.) and `ccas.analysis.tables.subtypes` for internal DB surrogate IDs (`ApiResponseBodyId`, `ApiResponseCacheId`), the latter with a stricter `> 0L` validator since `BIGSERIAL` keys start at 1.

**Enum serialization:** Enums extend `EnumJson[T]` for JSON (snake_case wire format, PascalCase in Scala) and/or `EnumSql[T]` for database persistence. Both provide codec instances via `given`. Some enums override `jsonToEnum` for non-standard Chess.com API mappings (e.g., `"closed:fair_play_violations"` → `Fairplay`).

**JSON decoding trait:** API model companions extend `JsonDecoding[T]`, which wraps `JsonDecoder` with convenience methods (`decodeZIO`, string extensions). The derived decoder is provided via `jsonDecoderDerived`. API models use `@jsonMemberNames(SnakeCase)` for field mapping. Global decoders for `Instant` and `URL` are exported automatically.

**ZIO effect types:** Helper functions `connectZIO` and `transactZIO` (in `ccas.utils.sql.PostgresClient` companion) provide the bridge between Magnum's context-function-based API and ZIO effects, returning `ZIO[PostgresClient, SQLException, A]`. For multi-statement atomic operations, `withTransaction` runs multiple `connectZIO` calls in a single JDBC transaction (commits on success, rolls back on any failure or interruption) by sharing a proxied connection via a scoped `PostgresClient`. All three functions retry automatically on transient connection errors (SQLState `08xxx`).

### HTTP Client

`ChessComClient` wraps a custom zio-http `Client` layer (`HttpClientLayer.live` in `ccas.utils.client`) for making GET requests with automatic JSON decoding. All CCAS apps and `CcasServer` provide `HttpClientLayer.live` rather than `Client.default` so transport-level settings (gzip, connection-pool shape, future HTTP/2) live in one place. Features include:
- Gate-based admission control with configurable concurrency limit (default 8 permits)
- Adaptive rate limiting: EMA-based request spacing with configurable minimum delay floor, failure-window throttle-down on 429 responses, time-gated recovery tiers
- Cloudflare challenge detection: immediate hard throttle to 1 permit on CF 403, independent of the failure window
- Per-attempt failure recording in `api_fetch_failure` with deduplicated response bodies in `api_response_body`
- Separate retry schedules for 429 (exponential backoff), Cloudflare 403 (fixed delay), and connection errors (exponential backoff); non-Cloudflare 403 and 404 are never retried
- Requires `CCAS_CONTACT_EMAIL` environment variable for the `User-Agent` header
- `Accept-Encoding: gzip` on every request; `HttpClientLayer` configures `Decompression.NonStrict` so Netty's `HttpContentDecompressor` decodes compressed bodies transparently. HTTP/2 is not yet available in zio-http 3.10.1 (tracked upstream at [zio/zio-http#3473](https://github.com/zio/zio-http/issues/3473)); `HttpClientLayer` is the single point of upgrade when it lands.
- Batch fetching via `getAll[T](urls)` using `ZIO.foreachPar` capped at `maxPermits` — network-bound fibers already bottleneck at the gate, and the cap bounds cache-warm fan-outs against the Hikari connection pool.
- The `followRedirects(3)` aspect's error handler returns 304 responses as-is (rather than failing on the missing `Location` header) so the cache's conditional-GET revalidation path works.

**Response caching (`getCacheable`):** Every successful fetch is persisted to `api_response_cache` (keyed by URL, with ETag / Last-Modified / `Cache-Control: max-age` / Content-Type metadata) and its body to `api_response_body` (SHA-256-deduped, so byte-identical responses across URLs share one row). `ChessComClient.getCacheable[T](url)` returns a `CacheableResult[T]` (`ccas.utils.client.CacheableResult`) with four variants:
- `Fresh` — served from cache with no network call; entry was within `Cache-Control: max-age`.
- `Revalidated` — conditional GET (`If-None-Match` / `If-Modified-Since`) returned 304 Not Modified; `fetched_at` is refreshed.
- `IdenticalBody` — 200 OK with a byte-identical body (SHA-256 dedup kept the same `body_id`).
- `Changed` — first fetch or actual content change; the new body replaces the cache row.

Each variant carries a lazy `getValue: Task[T]` so callers can branch on `isUnchanged` and skip downstream processing without triggering a body load or JSON decode. `get[T]` is a thin wrapper returning `T` via `getCacheable[T](url).flatMap(_.getValue)`. `CacheableResult` exposes `foldZIO(ifUnchanged)(ifChanged)` and `unlessUnchangedDiscard(zio)` for branching without hand-rolling a pattern match; dispatched via `final` overrides on a sealed `Unchanged[T]` intermediate supertype over the three hit variants, so T is compiler-verified end-to-end (no erased `@unchecked` match). Current callers: `HistoryProcessing.refreshSingleMatch` (Unchanged → bump `fetched_at`; Changed → full rework) and `HistorySeeding.seed{FromClubMatches,MatchesForPlayer,MatchesForPlayerAllClubs}` (Unchanged → skip the INSERT pipeline but still stamp `HistoryMemberQuery` where required). `Cache-Control: no-store` responses are not cached; `Cache-Control: no-cache` is honoured by persisting the entry with a cleared `max_age_seconds` so every subsequent request revalidates (RFC 7234 §5.2.2.2). ETag values are stored in wire format (`"..."` / `W/"..."`) and echoed back via `Header.Custom("If-None-Match", …)` to work around a quote-stripping bug in zio-http 3.10.1's `Header.IfNoneMatch.ETags.render`. `Last-Modified` is read via a raw-header lookup piped through `ccas.utils.HttpDate.parse` because Chess.com ships a non-RFC format (`Thursday, 16-Apr-2026 23:13:22 GMT+0000`, matching none of the three forms in RFC 7231 §7.1.1.1) that zio-http's typed `Header.LastModified` silently rejects; the parser tries Chess.com's shape first and falls back to IMF-fixdate / RFC 850 / asctime, and `If-Modified-Since` is always echoed in IMF-fixdate regardless of what was received. Empirically (2026-04-17) Chess.com's origin ignores `If-Modified-Since` regardless of format, so the conditional-GET path is in practice ETag-only. On a 304, `ApiResponseCache.touch` merges any refreshed `Cache-Control` / ETag / `Last-Modified` / Content-Type values from the 304 response using `COALESCE` for the validators (absent headers preserve stored values) and tri-state semantics for `max-age` (header absent → preserve; `no-cache` → clear; `max-age=n` → overwrite). Cache hits, 304 revalidations, and cache misses have dedicated counters on `StatsAccumulator` (persisted to `client_stats.cache_hits` / `cache_revalidations` / `cache_misses`) so the `requests` series stays an honest indicator of Chess.com API load. The three HistoryApp skip sites also record per-run domain-level counters on `history_run` (`refresh_match_unchanged` / `seed_club_matches_unchanged` / `seed_player_matches_unchanged`) so the short-circuits' impact on downstream DB / decode work is visible separately from transport-level cache savings.

**Cache retention:** `Tables.ensureTables` calls `ApiResponseCache.deleteBefore(now - retention)` on every app startup, chained with `ApiResponseBody.deleteOrphans` in the same transaction. The window comes from `chess-com-client.cache.retention-days` (default 30 in production, overridable via `CHESS_COM_API_CACHE_RETENTION_DAYS`; the test config uses a 100-year window so test fixtures that mock old timestamps aren't swept). Mid-flight races are tolerated: a `Fresh` / `Revalidated` result whose body was pruned by another process falls through to a recursive network refetch via `loadAndDecode`'s None-branch / `JsonDecodingException` recovery paths.

### Backend Server

`CcasServer` (`ccas.server`) is a zio-http server that exposes REST endpoints and runs background jobs:
- **Routes:** `HealthRoutes` (health/readiness), `JobRoutes` (submit and query jobs), `ScheduleRoutes` (CRUD for scheduled jobs). Route handlers use inline JSON codecs for request/response types. User-facing errors (`BadRequestException`, `NotFoundException`, `ConflictException`) form a sealed `UserFacingError` trait (`ccas.utils.errors`) that carries the HTTP status and a pre-encoded JSON body; `RouteHelpers.handleError` renders them uniformly (default body shape is `{"error": "<message>"}`, but `.of[B: JsonEncoder](body, msg)` companion constructors support structured payloads). Anything outside that hierarchy maps to a generic 500.
- **JobRunner:** Trait-based (`JobRunnerLive`) async executor that forks fibers per job, tracks state in `JobRun` table (ULID IDs via `ulid-creator`). Marks orphaned running jobs as failed on startup. The `submit` method takes an `Option[JobRunId] => RIO[..., Any]` effect function, passing the generated job run ID so analysis apps can link their run records back via `job_run_id`. Job kinds: `Recruitment`, `Membership`, `MatchRef`, `History`, `Stats`, `ClubData` (the latter has no HTTP route — scheduler only).
- **JobScheduler:** Polling daemon (configurable interval) that checks enabled `JobSchedule` entries and submits due jobs to the `JobRunner`.
- **ServerTables:** Ensures both analysis and server database tables exist on startup.

### Database

Uses Magnum (`com.augustnagro.magnum`) for SQL access with PostgreSQL. `PostgresClient` (`ccas.utils.sql`) wraps a Magnum `Transactor` backed by HikariCP and adds connection-pool hardening (keepalive probes, validation queries, lazy initialization) and transient-error retry (exponential backoff on SQLState `08xxx`). `PostgresClient.live` reads config from `application.conf` under the `database` prefix and provides a `PostgresClient` ZLayer; all app and server code depends on `PostgresClient` rather than `Transactor` directly. Custom `DbCodec` instances handle `Instant` (via `TIMESTAMPTZ`), `URL`, and `List[String]` (PostgreSQL arrays). Table names are derived from case class names via `CamelToSnakeCase` naming strategy. Server tables (`JobRun`, `JobSchedule`) reference clubs by `club_id` FK; route handlers resolve the slug from HTTP requests to a `ClubId` before submitting jobs. Analysis run tables (`MembershipRun`, `RecruitmentRun`, `HistoryRun`) have an optional `job_run_id` column linking back to the server-level job. Schema migrations for existing databases are managed via manual SQL scripts in the `sql/` directory.

### Test Data

API JSON test fixtures live in `data/test/api/*.json`. Tests in `TestApiJsonParsing` validate that these parse correctly into API model types.
