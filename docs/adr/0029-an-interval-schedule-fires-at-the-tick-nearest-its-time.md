# An interval schedule fires at the tick nearest its time

**Status:** Accepted, 2026-11-10 (#457).

## Context

`JobScheduler` ticks every poll interval (15 minutes by default) and fires an `Interval` schedule once `interval_hours` have passed since its `last_run_at`. It records `last_run_at` from the clock after the tick's read, which can wait seconds for a suspended Neon compute to start. So the tick exactly one interval later came a moment short, and every run waited one more poll. In production's `job_run`, scheduled MatchRef runs on consecutive days between 2026-10-12 and 2026-11-10 started 1 s to 26 min later than the day before, 9.6 min on average: a daily job walked round the clock. Ticks are not evenly spaced either: the laptop production runs on stretches them when it sleeps.

## Decision

**An interval schedule is due half a poll interval before its time**, so the tick nearest that time fires it, whichever side of it the tick falls. `last_run_at` still records when the job ran. `JobSchedule.isDue` takes the poll interval rather than a grace, and derives both tolerances from it: this half-poll and cron `Skip`'s two-poll grace.

## Alternatives rejected

- **Anchor to the slot:** record the due time a run belonged to rather than the clock, so the phase never moves. After an outage the catch-up run and the next on-phase run can fall close together: a schedule last run Monday at 15:24, with the server off until Wednesday 14:00, would run at 14:00 and again at 15:24. It also makes interval schedules a second cron, which already pins a job to an hour.
- **Record the tick's own instant, not the clock after the read, with a tolerance of a second or so.** It would never fire early, but it only removes the read's delay: a tick still lands milliseconds after its scheduled instant, and sleep moves ticks by minutes, so it needs a tolerance with no setting behind it. Half a poll is the one the scheduler's resolution already sets.

## Consequences

- Runs of an interval schedule are within half a poll of `interval_hours` apart. When the interval is a whole number of polls, as a whole number of hours is with the default 15 minutes, steady uptime keeps them to the same tick. Otherwise each run lands on the tick nearest its time, so the phase still moves, and the spacing averages out at whole polls rather than the interval: a 1-hour schedule polled every 25 minutes runs every 50.
- A job that skips what it refreshed within its own interval, such as ClubData with `minAgeHours` equal to `interval_hours`, finds almost everything too recent when it runs on time. Give it a minimum age below the interval.
- An outage still moves the phase: the job runs at the first tick after the server is back, and counts from there. A job that must keep to an hour needs a cron schedule.
- Every existing interval schedule's next run comes up to half a poll earlier than before, once. Nothing stored changes, so rolling back means reverting the code.
