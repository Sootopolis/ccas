# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

This file holds **rules and orientation** — the things you must not get wrong, and enough of a map to
find the rest. What each part *is* lives in [`docs/architecture.md`](docs/architecture.md); *why* it
is that way lives in [`docs/adr/`](docs/adr/); how to install, run and configure it lives in
[`README.md`](README.md). See [`docs/documentation-standard.md`](docs/documentation-standard.md) for
why the split exists and what belongs where.

## Project Overview

CCAS (Chess Club Admin System) pulls data from the Chess.com public API into PostgreSQL for chess
club administration: membership tracking, member performance analysis, and scouting/recruiting
players from other clubs. It includes a backend HTTP server with job scheduling, and a CLI.

## Build & Test Commands

An sbt project (Scala 3, JDK 25 LTS). Exact versions live in `project/Versions.scala`,
`project/build.properties` and `.sdkmanrc` — they are not mirrored here, because a prose copy drifts
on the next bump (it already had).

The JDK is pinned in `.sdkmanrc` — run `sdk env` in the repo root, or `sdk env install` on a fresh
machine. It is pinned because it drifted once: a Homebrew-backed SDKMAN candidate dangled when
Homebrew removed the keg it symlinked into, and every build then failed with "Unable to locate a Java
Runtime". If a build fails that way, check `java -version` before anything else.

- **Compile:** `sbt compile`
- **Run all tests:** `sbt test`
- **Run a single test suite:** `sbt "testOnly ccas.api.TestApiJsonParsing"` (fully qualified object name)
- **Continuous compile on change:** `sbt ~compile`
- **Interactive SBT shell:** `sbt` then run commands without the `sbt` prefix

Tests use ZIO Test (`ZIOSpecDefault`). SQL tests require a running PostgreSQL instance with a
`ccas_test` database (see `src/test/resources/application.conf`). A tracked `pre-push` hook runs the
full suite before every push — enable it per clone with `git config core.hooksPath .githooks`.

## Conventions

Rules that aren't derivable from reading the code. Follow them; they exist because the alternative was tried.

**Git.** Never `git commit`, `git push`, or open a PR without the developer's explicit go-ahead **in that message**. Finish the work, run the suite, and leave the changes uncommitted in the working tree; then report and stop. Review happens on the working-tree diff, and committing buries the change behind commit boundaries where it is much harder to examine — a commit message also asserts what a change does while it is still under review. Announcing an intention to commit earlier in the session is not approval; neither is a background-task notification, nor approval of an earlier commit. Approval is per-action: "commit this" is not "push this", and neither is "open a PR". Offer the commit rather than making it. If a commit has already happened unapproved, say so and offer to `git reset --soft` back to the base branch. Never commit while `wip` is checked out either — cut a feature branch first with `git switch -c <type>/<slug>`, which carries uncommitted work across. `wip` is a local parking branch that must only ever lag `main`; a commit on it turns re-parking from a fast-forward into a judgement call about whether that commit still exists anywhere else.

**Documentation.** Follow [`docs/documentation-standard.md`](docs/documentation-standard.md); `scripts/check-docs.py` enforces the mechanical half from `pre-push`, including this file's own word budget. Length ceilings are judgement calls — waive one inline with `docs-standard: allow long-block -- <reason>` rather than contorting the code, and never reach for `--no-verify`. `--report` prints the review agenda. Nothing in that document outranks the work. One fact, one home: scaladoc for how to call a thing, `//` for why a line is strange, an ADR for why X over Y, README for how to run it, the test name for what is guaranteed. Never restate a machine-readable fact (versions, defaults, schema) in prose — point at the definition. Comment budgets: `//` ≤ 3 lines, member scaladoc ≤ 10, class scaladoc ≤ 15. Over budget means the knowledge belongs in an ADR, with a one-line pointer left in the code. Scala already says most of it — do not narrate a `sealed trait`'s variants, an `Option`, or an error channel back to the reader.

**Scala style.** Braces on `match`, parens on `if`/`else`; no Scala 3 braceless (indentation-only) syntax. All imports at the top of the file, never mid-file. Prefer a for-comprehension over a chain of 3+ ZIO combinators, especially inside a `catchAll` body. Don't put large blocks inside `fold` / `foldZIO` / `mapBoth` bodies — extract a named method. Name arguments when a construction is already one field per line, above all for adjacent same-typed `None` / `Some` fields. No default argument values unless a caller genuinely needs them: this is an application, not a library. Never embed a raw control byte (ESC and friends) in a string literal — use `\uXXXX`.

**ZIO idioms.** `ZIO.whenDiscard` when the result is `Unit`. Prefer the `when` variants over the `unless` ones and negate the predicate instead — a reader should be able to follow the condition without mentally inverting it. Prefer `ZIO.whenZIODiscard(effect)(body)` over `effect.flatMap(b => ZIO.whenDiscard(b)(body))` whenever the bound value is used only as the predicate (`.negate` covers the inverted case); keep the `flatMap` only when the body also needs the value. `layer.build *> rest` when a layer is provided purely for its side effect (silences the `ZLayer` macro warning). Note that in ZIO 2.1 `ZIO.logInfo` and friends ignore `currentLogLevel`, so any per-fiber level filter has to live in the `ZLogger` itself.

**SQL.** Write methods return the number of rows affected, not `Unit`. Prefer `UNION ALL` over `UNION` where deduplication isn't needed. Magnum specifics: a custom `DbCodec` must not throw on `null`, because Magnum's `OptionCodec` reads the value *before* checking `wasNull`; and interpolating a bare enum case (`${TriggerType.Interval}`) into `sql""` emits no placeholder and produces a syntax error — widen the value to the enum type first.

**Formatting.** The repo is not scalafmt-clean. Never run `scalafmtAll` / `scalafmtSbt` — it churns ~120 files. Format only the files you touched, and even then check the diff: reformatting a file that was never formatted rewraps comments and reorders imports far beyond your change.

**Tests.** `Test/parallelExecution := false` is deliberate — several suites touch process-global state or a shared schema. Within a suite ZIO Test still runs tests in parallel, so a suite sharing mutable schema or filesystem state needs `@@ TestAspect.sequential`. Fix flakes at the root; `@@ TestAspect.flaky` is not an acceptable remedy. Tests run against `ccas_test`; `FreshSchemaLayer` refuses to reset a schema in any database not named `*_test`, because an exported `DATABASE_URL` redirects the whole suite (classpath `application.conf` merge — see the comment in `src/test/resources/application.conf`) and the target could be production.

**Output files.** Apps that write reports go through `OutputFile` (`ccas.utils`), which owns the layout and archives any previous file for the same app into an `archive/` subdirectory first. Club-scoped output uses `write` / `writeAndLog` → `out/{clubSlug}/{timestamp}-{appName}.{ext}`, grouped by club so one club's history reads in order; output with no single club uses `writeGlobal` / `writeAndLogGlobal`, which takes an explicit subdirectory → `out/{subDir}/{timestamp}-{appName}.{ext}`. Don't hand-roll paths.

**Chess.com API reliability.** Three fields lie, and code must not branch on them: a tournament's `status`, `ApiClub.lastActivity` (observed 12+ years off on active clubs), and the match endpoint's view of a match (it lags the game archives — prefer archive data). A 404 is not permanent: `"X not found"` bodies are timeline-unstable, and usernames and club slugs can flip 404 → 200 when a handle is registered later. High 404 counts on cancelled matches are expected noise.

**Schema changes.** The `createTable` definitions in code are the schema of record — there is no migration framework. Apply a change by editing the `CREATE TABLE` and running the equivalent `ALTER` against each existing database by hand (`psql`). `sql/` holds dated scripts from earlier changes and is kept for history; new changes are not scripted there unless the change is intricate enough to be worth replaying. Treat that directory as an archive, not a ledger you must append to.

**Deployment.** One `CcasServer` per database is the supported model. A CLI invocation alongside the server on the same DB is fine — the CLI builds no `JobRunner`, so it never mutates scheduled-job state. Two or more servers against one DB is unsupported: `JobRun.markOrphansAsFailed` fails *all* `Running` jobs on startup with no instance-ownership filter (#110), and the rate limiter is per-process (#111). Scheduler double-fire *is* prevented, and all seeders are idempotent. Multi-server hosting is #60, gated on #110 + #111.

**Shell completions.** `completions/ccas.bash` is committed and asserted byte-equal to `CompletionEmitter.bash` by `TestCcasCompletion`. Regenerate after any CLI tree change with
`sbt --server -batch -error 'runMain ccas.cli.Main completion bash' > completions/ccas.bash`.
`--server` is load-bearing: with `SBT_NATIVE_CLIENT=true`, `-batch` alone still routes through the
thin client, which writes banner lines and a raw ESC into **stdout**, corrupting the redirect.

**zio-cli argument parsing.** zio-cli swallows an option written *after* a positional as another positional value, and `Args.atMost(n)` silently *truncates* extra values rather than rejecting them. Together those turned `ccas use-club team-alpha --clear` into a silent *set* of the club the user asked to clear. Capture every positional with `.repeat` and validate arity by hand (see `UseClub`) wherever a dropped argument would change what the command does.

## Architecture

### Packages

1. **`ccas.api`** — Chess.com API models and client. Case classes model API JSON responses
   (`ApiPlayer`, `ApiClub`, `ApiDailyMatch`, …); each companion extends `JsonDecoding[T]`. These are
   read-only DTOs and are never written to the database directly.
2. **`ccas.analysis`** — domain tables and business logic. `analysis.tables` holds the persisted
   entities (core, ref resolution, recruitment, history crawl, run tracking, `AppSetting`, and API
   diagnostics/caching). `analysis.apps` holds the runnable applications — `MembershipApp`,
   `RecruitmentApp`, `RecruitmentCriteriaApp`, `RefApp`, `HistoryApp`, `StatsApp`, `ClubDataApp` —
   and shared helpers (`PlayerUpdater`, `UsernameRenameResolver`, `ClubSlugRenameResolver`).
3. **`ccas.server`** — zio-http backend. `server.jobs` (`JobRunner`, `JobRun`, `JobSchedule`),
   `server.routes` (jobs, schedules, blacklist, recruitment criteria, health),
   `server.scheduler` (`JobScheduler`). Entry point `CcasServer extends ZIOAppDefault`.
4. **`ccas.utils`** — shared infrastructure: HTTP client (`client/`), JSON traits (`json/`), SQL
   client and helpers (`sql/`), opaque-type utilities (`opaque/`), pretty-printing.

`ccas.cli` (`ccas.cli.Main`) is the zio-cli binary: `CliCommand` defines the tree, `Dispatcher` turns
each parsed command into HTTP calls against a running server, `CompletionSpec` / `CompletionEmitter`
generate shell completions. Exit codes: 0 success/help, 1 job failure, 2 usage error.

### Key patterns

**Table entities (Magnum).** A case class annotated `@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)`
and `derives DbCodec`; its companion provides `createTable`, `selectAll`, `selectId`, `insert`,
`insertBatch`, `update`, `upsert`. All SQL wraps Magnum queries in `PostgresClient.connectZIO`
(reads) or `transactZIO` (writes). Complex queries use `SqlLiteral` for reusable column lists. Some
entities also use Magnum's `Repo[T, T, ID]` / `ImmutableRepo[T, ID]`.

**Opaque types.** Domain IDs and constrained values are Scala 3 opaque types with companion traits
(`StringCompanion`, `StringKeyCompanion`, `IntCompanion`, `LongCompanion`, `DoubleCompanion`) in
`ccas.utils.opaque`, each providing `JsonCodec`, `DbCodec` and `DeriveConfig`, plus optional
`validateRaw` / `normalize`. Chess.com domain IDs live in `ccas.api.misc.subtypes`; internal DB
surrogate IDs in `ccas.analysis.tables.subtypes`, the latter with a stricter `> 0L` validator since
`BIGSERIAL` keys start at 1.

**Enums.** `EnumJson[T]` for JSON (snake_case wire, PascalCase in Scala) and/or `EnumSql[T]` for the
database; both supply codecs via `given`. Some override `jsonToEnum` for non-standard Chess.com
mappings (`"closed:fair_play_violations"` → `Fairplay`).

**ZIO effect types.** `connectZIO` and `transactZIO` (`ccas.utils.sql.PostgresClient` companion)
bridge Magnum's context-function API to `ZIO[PostgresClient, SQLException, A]`. `withTransaction`
runs several `connectZIO` calls in one JDBC transaction via a shared proxied connection. All three
run on `attemptBlockingInterrupt`, so interruption on cancel/shutdown aborts a fiber parked on pool
checkout, and retry on transient connection errors (SQLState `08xxx`) — except a Hikari pool-checkout
timeout, which fails fast rather than re-blocking for another `connectionTimeout`.

**Database URLs.** `database.url` (`DATABASE_URL`) passes through `PostgresClient.normalizeJdbcUrl`,
which accepts both the JDBC form and the libpq URI managed providers hand out
(`postgresql://user:pass@host/db`). Credentials are lifted out of the URL into Hikari's
`setUsername` / `setPassword` for both forms, and percent-decoding differs by position on purpose —
[0014](docs/adr/0014-accept-both-database-url-forms.md) holds why, and says not to unify it.

### Where the rationale lives

| Area | ADR |
| --- | --- |
| Adaptive rate limiting (current design) | [0012](docs/adr/0012-gate-based-adaptive-throttle.md), superseding [0001](docs/adr/0001-adaptive-rate-limiting.md) |
| Dependency decisions, sbt 2 | [0002](docs/adr/0002-dependency-decisions-2026-08.md), [0003](docs/adr/0003-defer-sbt-2.md) |
| API fan-out concurrency cap | [0004](docs/adr/0004-api-fan-out-concurrency-cap.md) |
| The zio-http client layer (gzip, timeouts, pool) | [0005](docs/adr/0005-own-the-http-client-layer.md) |
| What the pacing EMA measures | [0006](docs/adr/0006-pacing-ema-measures-the-http-exchange-only.md) |
| Response caching, conditional GETs, retention | [0007](docs/adr/0007-response-caching-in-postgres.md) |
| Body storage outside Postgres | [0008](docs/adr/0008-body-store-outside-postgres.md) |
| `BodyStore` deadlines and the S3 budget | [0009](docs/adr/0009-bound-every-body-store-operation.md) |
| Username / club-slug rename recovery | [0010](docs/adr/0010-rename-recovery-for-usernames-and-club-slugs.md) |
| CLI locality, `current_club`, config files | [0011](docs/adr/0011-cli-locality-and-the-current-club-pointer.md) |
| Job-log sink surviving write failures | [0013](docs/adr/0013-job-log-sink-survives-write-failures.md) |
| Both `DATABASE_URL` forms, credential lifting | [0014](docs/adr/0014-accept-both-database-url-forms.md) |
| The server read-idle reaper | [0015](docs/adr/0015-server-read-idle-reaper.md) |

Component-level detail — the apps and their run modes, the route surface, `JobRunner` cancellation
semantics, the scheduler, `app_setting` — is in [`docs/architecture.md`](docs/architecture.md).

Read the relevant ADR **before** changing anything in `ccas.utils.client`, the cache tables, or the
CLI command tree. Each one records a trade-off that was argued and, in several cases, a fix that was
tried and reverted.

### Test data

API JSON fixtures live in `data/test/api/*.json`; `TestApiJsonParsing` asserts they parse into the
API model types.
