# ChessComClient — parked follow-ups

Noted during the 2026-04-14 `client_stats` analysis (see
`~/.claude/plans/modular-questing-island.md` for the full write-up and supporting data).
The `.env` override was reverted to the 60 ms default in that pass; these items are
everything that was deliberately **not** addressed.

## Per-app `min_request_delay_ms` override

The weighted `ema_delay_ms / requests` metric shows the floor is binding on essentially
every request for history, ref, clubdata, and recruitment. But each workload has very
different 429 risk:

| app         | cfg 4 (60 ms) req | cfg 4 e429 | rate    |
|-------------|-------------------|------------|---------|
| history     | 32 805            | 0          | 0.000 % |
| recruitment | 4 908             | 0          | 0.000 % |
| ref         | 20 879            | 78         | 0.372 % |
| clubdata    | 22 995            | 176        | 0.760 % |

One global floor leaves throughput on the table for history/recruitment (which are proven
safe) while still being at the margin for ref/clubdata. A per-app override would let history
run at 40–45 ms (biggest wall-clock win) while keeping ref/clubdata conservative at 60 ms.

Sketch: add `minDelayMsOverride: Option[Long]` to `ChessComClient.live(label, …)`, wire it
into the `ClientConfig` that gets hashed. Different floors produce different `config_id`s
so stats stay comparable.

Files: `ChessComClient.scala`, each `*App.scala` entry point.

## `errors_other` overstates failure count

`errors_other` lumps expected 404s (1249 rows in `api_fetch_failure`, mostly cancelled
matches per the existing memory) with real failures (~20: 500s, channel closures, decoding
errors). The `StatsAccumulator.summary` line printed at run end therefore reports things
like "539 failures" when 539 of them are expected 404s from the cancelled-match backlog.

Fix: split into `errors_expected_404` vs. `errors_unexpected` in `StatsAccumulator` and
`client_stats` (schema migration required).

## No FK from `client_stats` to run tables

Correlating a `client_stats` session to its `MembershipRun` / `RecruitmentRun` / `HistoryRun`
today requires joining on `app_label` + timestamp window. Works for CLI runs because each
session is one run, but fragile. If/when jobs run under `CcasServer` (shared client, label
`server`), one session will cover many runs and correlation breaks entirely.

Fix: add a nullable `job_run_id` column to `client_stats`, populated via the `Option[JobRunId]`
that `JobRunner.submit` already passes to analysis apps.

## Chess.com 500s on `club/botvinnik-chess-school/matches`

Three distinct `clubdata` sessions (2026-04-13 00:44, 2026-04-13 17:30, 2026-04-14 23:02)
hit the exact same endpoint and got HTTP 500 every time. Looks like a persistent
server-side bug on that specific club. Candidate for `ClubRefSkip` or equivalent quarantine
so we stop retrying it.
