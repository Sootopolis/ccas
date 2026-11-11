# A cron schedule fires each wall-clock time once

**Status:** Accepted, 2026-11-11 (#458).

## Context

A cron schedule names wall-clock times in an IANA zone, and a zone with daylight saving repeats an hour each autumn and skips one each spring. `JobSchedule.nextDue` takes cron4s's `next`, which counts in wall-clock time, and `JobSchedule.isDue` takes its `prev`, which keeps the offset of the instant it is given. Across a clock change the two disagreed, and since [0028](0028-the-scheduler-reads-only-when-a-schedule-can-be-due.md) the scheduler reads only when `nextDue` says so, or when something else happens to read. In `Europe/London`, with cron4s 0.8.2:

- **Clocks go back, 25 October 2026.** A daily `0 30 1 * * ?` runs at 01:30 BST (00:30 UTC), and `next` goes to 01:30 the following day. But `prev` at 06:30 UTC gives 01:30 GMT, the repeat, which is after the run, so the next read fired a `CatchUp` schedule again, five hours late.
- **Clocks go forward, 29 March 2026.** 01:30 does not exist; java.time resolves it to 02:30 BST. `prev` returns that boundary from 02:00 BST onwards, before it comes, so a read between 02:00 and 02:29 BST fired the schedule early, and a later read fired it again for the boundary at 02:30: a `CatchUp` schedule at any later read, a `Skip` one within its grace.

No cron schedule existed in production when this was found.

## Decision

**Each wall-clock time fires at most once.** `isDue` compares a boundary with `last_run_at` in the schedule's own zone, so the repeat of a local time already run counts as run, and it treats a boundary after `now` as not yet due. A time the clocks skip fires at the instant java.time gives it, an hour later on the clock. That is what cron4s's `next` does from before either change. Asked from inside one, `nextDue` corrects it: in the skipped hour's aftermath it returns the shifted boundary, which `next` steps past, and in the repeated hour it counts from the run's time on the clock, so it passes over times already run. `isDue` and `nextDue` then agree on both nights; `TestJobScheduleIsDue` checks them a minute apart through each.

| Cron | Clocks go back | Clocks go forward |
| --- | --- | --- |
| `0 30 1 * * ?` | 01:30 BST only | 02:30 BST |
| `0 0 * * * ?` | 01:00 BST, then 02:00 GMT | 00:00 GMT, then 02:00 BST |
| `0 */15 * * * ?` | nothing in the repeated hour | every quarter hour the clock shows |

## Alternatives rejected

- **Fire every instant whose local time matches.** That was the behaviour before 0028: a daily job ran twice in the repeated hour. It would also need `nextDue` to step through that hour itself, since cron4s's `next` passes over it.
- **FreeBSD cron's rule** (`cron -s`): a job that runs hourly or more often runs through the repeated hour as usual, and a less frequent one runs once. It keeps an hourly job's cadence, but needs the same stepping in `nextDue` (from 01:45 BST, `*/15` gives 02:00 GMT), plus a classification of crons by their fields. CCAS's jobs are sweeps that take minutes to hours, and a job that needs a steady cadence in elapsed time has interval schedules, which count real hours.

## Consequences

- Once a year, an hourly cron in a zone with daylight saving leaves a two-hour gap, and a sub-hourly one runs nothing for an hour.
- A cron whose time the clocks skip runs an hour later by the clock that night, and at its usual time the next. A `CatchUp` schedule that comes back from downtime in that hour waits for the shifted boundary rather than firing at once, since that boundary is the one it catches up to.
- The `Skip` grace and `CatchUp`'s lateness still measure elapsed time, not wall-clock time.
- Nothing stored changes, so rolling back means reverting the code.
