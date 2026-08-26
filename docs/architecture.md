# Architecture reference

What each moving part is and how the pieces connect. Reference, not rationale — the *why* behind a
design lives in [`adr/`](adr/), the rules for working in the code live in
[`../CLAUDE.md`](../CLAUDE.md), and installation and usage live in [`../README.md`](../README.md).

## Applications (`ccas.analysis.apps`)

Runnable apps, each invocable from the CLI and most also submittable as a server job.

| App | `JobKind` | Notes |
| --- | --- | --- |
| `MembershipApp` | `Membership` | reconciles club membership |
| `RecruitmentApp` | `Recruitment` | scouts candidates against stored criteria |
| `RefApp` | `MatchRef` | resolves match/tournament references |
| `HistoryApp` | `History` | BFS crawl of a club's match history |
| `StatsApp` | `Stats` | per-player contribution stats |
| `ClubDataApp` | `ClubData` | refreshes club profile data; **no HTTP route** — CLI or scheduler only |
| `BlacklistApp` | — | not a `JobKind`; runs inline from the CLI or `BlacklistRoutes` |
| `RecruitmentCriteriaApp` | — | not a `JobKind`; runs inline from the CLI or `RecruitmentCriteriaRoutes` |

The two inline apps bypass `JobRunner` deliberately: their mutations are small enough to handle
synchronously rather than as async jobs.

### `HistoryApp` run modes

- **Default (active-only, incremental)** — queries only members whose match lists have not been
  fetched, and re-queues only actively-changing matches (Registration + InProgress). Stored-Finished
  matches are not re-fetched, since Finished data is effectively immutable, but genuinely-new matches
  of any status are still ingested via the listing endpoints. Cheapest mode; use for scheduled runs.
- **`--include-finished`** — also re-queues stored matches that finished within the 90-day stale
  window. Use periodically to pick up rare organiser corrections.
- **`--full`** — clears member query history so every member's match list is re-fetched.
- **`--refresh [hours]`** — after BFS, re-fetches settled matches (finished and past the stale
  window) straight from `club_match`, bypassing the pending table. With `hours`, only those whose
  `fetchedAt` is older than that. Resumability falls out of that filter: a refreshed match has a new
  `fetchedAt` and is skipped next time, while a *failed* one keeps its old `fetchedAt` and is
  retried.

Phases: **Initialize** (reconcile membership, load state, reset pending statuses, create a
`HistoryRun`) → **Seed** (collect match IDs into `history_pending_match` from the club matches
endpoint, each member's match list, and stale existing matches; retry previously unresolved clubs and
board players) → **Process** (BFS wave loop: fetch and persist in parallel batches, discover unknown
players, seed their match lists, repeat until no pending matches remain) → **Refresh** (only with
`--refresh`) → **Finalize** (mark the run complete, log stats, write a report).

Entities that cannot be resolved are recorded rather than dropped: an unresolvable team club URL goes
to `unresolved_match_club` (and if *neither* team resolves to the target club the match is marked
`Unidentified` — data saved, BFS expansion skipped), and an unresolvable board username goes to
`unresolved_board_player` with the board row saved under a `None` player ID. Both are retried at the
start of each run and patched in place on success.

When several club slugs are given in one CLI invocation, clubs run sequentially against a shared
`SharedContext` that removes redundant API calls: unresolved retries run once rather than per club;
a member's fetched match list is seeded into every other club's pending queue; matches already
processed by an earlier club are skipped without re-fetching; and already-processed matches are
filtered out of stale seeding. `history_run.matches_processed` counts processed plus shared-skipped so
run totals stay comparable. This applies to the CLI multi-club path only — API-submitted jobs run
independently per club.

### `RecruitmentCriteriaApp`

Subcommands: `set <club-slug> <alias> [--json <file>]`, `show <club-slug> <alias>`,
`list <club-slug>`, `sample`.

A "set" is a **versioned insert**. Criteria rows are immutable and `recruitment_alias`'s primary key
is `(club_id, alias, since)`, so the same operation covers first-set and later-change;
`RecruitmentApp` reads newest-wins via `selectLatest`. The app skips the insert when the capped
incoming criteria equals the latest stored, uses `RecruitmentAlias.upsert` so a same-instant re-set
repoints the row rather than colliding on the composite key, requires the club to exist locally (no
`ChessComClient` dependency), and logs a per-field diff on every save. Interactive prompts pre-fill
from the existing alias — or `RecruitmentCriteria.defaultDaily` for a new one — preview a diff, and
require a `Save? [Y/n]` confirmation. The `CriteriaSpec` DTO (`RecruitmentCriteria` minus
`criteria_id`) is the shared wire shape for both the `--json` file and the HTTP body.

## Server (`ccas.server`)

`CcasServer extends ZIOAppDefault`. `ServerTables` ensures both analysis and server tables exist on
startup.

### Routes

- `HealthRoutes` — health and readiness.
- `JobRoutes` — submit, query and cancel jobs, plus recruitment-result delivery:
  `GET /api/jobs/{jobId}/recruitment/invited` (paste-ready invited usernames),
  `GET .../recruitment/found` (still-`Deferred` candidates),
  `POST .../recruitment/confirm` (flips that run's `Deferred` candidates to `Invited` in one
  transaction, returning `ConfirmResult{marked, usernames}`), and
  `GET /api/recruitment/clubs/{slug}/latest/invited` +
  `GET /api/recruitment/runs/{runId}/invited` for reporting a past run. These drive `ccas recruit`'s
  `--stdout`, interactive-confirm and `--report` modes; an interactive scout sends
  `autoConfirm=false`, so candidates stay `Deferred` until the operator confirms.
- `ScheduleRoutes` — CRUD for scheduled jobs.
- `BlacklistRoutes` — synchronous CRUD for `RecruitmentBlacklist`, delegating to `BlacklistApp`.
- `RecruitmentCriteriaRoutes` — synchronous `POST /api/recruitment-criteria`, `GET .../{slug}/{alias}`,
  `GET .../{slug}`, delegating to `RecruitmentCriteriaApp`.

Route handlers use inline JSON codecs. User-facing errors (`BadRequestException`,
`NotFoundException`, `ConflictException`) form a sealed `UserFacingError` trait (`ccas.utils.errors`)
carrying the HTTP status and a pre-encoded JSON body; `RouteHelpers.withErrorHandling` renders them
uniformly (default `{"error": "<message>"}`, with `.of[B: JsonEncoder](body, msg)` for structured
payloads). An escaping `HttpStatusException` from the Chess.com client renders as 502. Anything else,
defects included, collapses to a generic 500 with the full cause logged via `ZIO.logErrorCause`;
pure-interrupt causes are re-propagated so shutdown and client-disconnect noise stays out of the
error log.

### `JobRunner`

Trait-based (`JobRunnerLive`) async executor. Forks a fiber per job and tracks state in `JobRun`
(ULID IDs via `ulid-creator`). `submit` takes an `Option[JobRunId] => RIO[..., Any]`, passing the
generated job run ID so analysis apps can link their run records back via `job_run_id`.
`JobRunStatus` is `Running | Completed | Failed | Cancelled`.

**Cancellation.** `cancel(id)` interrupts the job's forked fiber. Handles are retained per-process in
a `runningFibers` map, registered *before* the fork so de-registration cannot race registration. It
adds the id to a `cancelRequested` set and then `interruptFork`s; the job's own `.onInterrupt` records
`Cancelled` **only if** its id is in that set. That is what separates an operator cancellation from
the `layerScope` interrupt fired at every in-flight job on server **shutdown** — those are left
`Running`, so the next boot's `markOrphansAsFailed` records them as `Failed` / "Service restarted",
which also covers hard crashes where finalizers never run.

Cancellation is best-effort and asynchronous. The DB helpers run on `attemptBlockingInterrupt`, so a
fiber parked on pool checkout is interrupted promptly, but an *executing* blocking JDBC statement
runs to completion first — a `socket.read()` ignores `Thread.interrupt`, bounded by `socketTimeout`.
The `markCancelled` `WHERE status = Running` guard makes the terminal write a no-op if the job
reached Completed or Failed first.

All `completed_at` writers (`updateStatus`, `markCancelled`, `markOrphansAsFailed`) take an `Instant`
from the caller's `Clock.instant` rather than SQL `NOW()`, so every job timestamp comes from the one
testable app clock and stays coherent with `started_at`.

The CLI surface is `ccas cancel <job-id>`. A job cancelled out from under an active `ccas logs`
follow renders "was cancelled" and exits non-zero.

### `JobScheduler`

A polling daemon on a configurable interval that checks enabled `JobSchedule` entries and submits due
jobs to the `JobRunner`.

## Database

Magnum (`com.augustnagro.magnum`) over PostgreSQL. `PostgresClient` (`ccas.utils.sql`) wraps a Magnum
`Transactor` backed by HikariCP, adding connection-pool hardening (keepalive probes, validation
queries, lazy initialization) and transient-error retry. `PostgresClient.live` reads config under the
`database` prefix; all app and server code depends on `PostgresClient`, never on `Transactor`.

Custom `DbCodec` instances handle `Instant` (via `TIMESTAMPTZ`), `URL`, and `List[String]` (PostgreSQL
arrays). Table names derive from case class names via the `CamelToSnakeCase` naming strategy.

Server tables (`JobRun`, `JobSchedule`) reference clubs by `club_id` FK; route handlers resolve the
slug from the request to a `ClubId` before submitting. Analysis run tables (`MembershipRun`,
`RecruitmentRun`, `HistoryRun`) carry an optional `job_run_id` linking back to the server-level job.

`app_setting (key TEXT PK, value TEXT)` is a generic per-key store for DB-owned, app-wide policy —
runtime-tunable without a redeploy and consistent across every process on one database, kept
orthogonal to `client_config` (per-process `ChessComClient` tuning). Typed access goes through
`AppSetting.Key[A]` and the companion registry (`AppSetting.CacheRetentionDays`, `AppSetting.all`),
each key carrying its default and string codec, so the stringly-typed table is confined to one place.
New keys reuse the shape with no schema churn. There is no CLI or route surface yet — change values
with SQL.

`scripts/backup-neon.sh` parses both URL forms and routes the password through `PGPASSWORD` so it
never reaches `argv`.
