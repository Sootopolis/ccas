# The scheduler reads its schedules only when one can be due

**Status:** Accepted, 2026-11-10 (#422). Amended by #458.

## Context

`JobScheduler` read `job_schedule` at every tick, every `SCHEDULER_POLL_MINUTES` (15 by default). On Neon each read wakes the compute, which stays up until five quiet minutes have passed. Once the pool stopped holding the compute awake (#420), that read was nearly all of an idle server's cost; the daily retention sweep is the rest. Neon's operations log from 2026-11-09 shows a wake every 15 minutes, each one 5m16s long, so the compute was awake 35% of idle time. That is about 65 of the Free plan's 100 CU-hours a month at full uptime, before any job runs. Production's four schedules are all daily, so the server needed the database once a day and read it 96 times.

## Decision

**The tick stays, and the read is gated.** The scheduler still wakes every poll interval, but in the JVM only. It reads the schedules when one of three things holds:
- the earliest `JobSchedule.nextDue` from its last read has passed;
- a write through the server asked it to `recheck`;
- `MaxReadGap` has passed since its last read.

A due schedule that did not fire, because its job is still running or its submission failed, keeps the next read at the next tick, as before. A failed read retries at the next tick. A row whose trigger or params won't decode can't fire until it is changed, so it is logged at each read but does not hold the next one early; the API stores `params` without decoding them, so such a row can arrive that way.

**Every server path that writes `job_schedule` asks for a recheck:** the schedule routes, and unmarking a managed club. Boot seeding runs before the scheduler starts. The CLI writes only through the server.

**`isDue` treats a cron boundary in `now`'s second as passed.** cron4s's `prev` is strict to the second, so within a boundary's second `isDue` read the boundary before it, and ticks that landed in that second fired only every other boundary. Once the read is gated, a read landing exactly on a boundary would set `nextDue` to the one after it, and a `Skip` schedule would lose a day.

## Alternatives rejected

- **A longer `SCHEDULER_POLL_MINUTES`.** It needs no code, but 60 minutes still keeps the compute awake 9% of the time (~17 CU-hours), and every job starts up to an hour late. It also stretches the `Skip` grace with it.
- **Sleep until the next due time.** On a Mac a sleep's timer stops while the machine sleeps, so one long sleep could wake hours late. Ticking and comparing the wall clock keeps lateness at one poll interval, as before.
- **`LISTEN`/`NOTIFY` on schedule changes.** A listening connection holds the compute awake, which is the cost being removed.

## Consequences

- **A write the server is not told about is seen within `MaxReadGap`:** a hand-edited row, a second server's write, or `ServerTables` run on its own to seed. That is a new dependence on one server, bounded by the gap (#220).
- **The idle cost is now a read per distinct due time plus one every `MaxReadGap`,** not one per tick. For production's four daily schedules that means a read when they come due and the gap's reads between them.
- While a due job can't start because its last run is still going, the scheduler reads at every tick, as before. A long History crawl overlapping its own next run keeps the compute awake for that time, which the running job does anyway.
- A read comes only when `nextDue` says a schedule can be due, so `isDue` must never hold before it. Across a clock change it did, and a cron fired twice; [0030](0030-a-cron-schedule-fires-each-wall-clock-time-once.md) settles what a cron means on those nights.
- Nothing stored changes, so rolling back means reverting the code.
