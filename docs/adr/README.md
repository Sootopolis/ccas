# Architecture decision records

Why a thing is the way it is, in the [Nygard lightweight
format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions): title, status,
context, decision, consequences. One decision per file, numbered in the order they were taken.

**ADRs are immutable.** A decision that is later reversed is not edited — it gets a new ADR, and the
old one's status becomes `Superseded by NNNN`. That is what makes an ADR safe to link to from a
comment: the target cannot quietly change out from under the pointer.

Write one when the decision is hard to reverse, involves a real trade-off, or has been argued twice.
Not for routine changes — git and the commit message already hold those. Date the status from git
(`git log -S<symbol> --reverse`), never from memory. See
[`../documentation-standard.md`](../documentation-standard.md) for how ADRs relate to comments,
scaladoc and the README.

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-adaptive-rate-limiting.md) | Adaptive rate limiting for `ChessComClient` | **Superseded by 0012**, 2026-04-01 |
| [0002](0002-dependency-decisions-2026-08.md) | Dependency decisions, 2026-08 | Accepted, 2026-08-25 |
| [0003](0003-defer-sbt-2.md) | Stay on sbt 1.x; defer sbt 2 | Deferred, 2026-08-25 |
| [0004](0004-api-fan-out-concurrency-cap.md) | Cap API fan-out at the DB pool, not just the gate | Accepted, 2026-06-23 |
| [0005](0005-own-the-http-client-layer.md) | Own the zio-http client layer | Accepted, 2026-04-16 |
| [0006](0006-pacing-ema-measures-the-http-exchange-only.md) | The pacing EMA measures the HTTP exchange only | Accepted, 2026-08-14 |
| [0007](0007-response-caching-in-postgres.md) | Cache API responses in Postgres, keyed by URL | Accepted, 2026-04-16 |
| [0008](0008-body-store-outside-postgres.md) | Keep response bodies out of Postgres | Accepted, 2026-08-07 |
| [0009](0009-bound-every-body-store-operation.md) | Bound every `BodyStore` operation in time | Accepted, 2026-08-14 |
| [0010](0010-rename-recovery-for-usernames-and-club-slugs.md) | Username / club-slug rename recovery | Accepted, 2026-05-07 |
| [0011](0011-cli-locality-and-the-current-club-pointer.md) | CLI locality, `current_club`, config files | Accepted, 2026-06-25 |
| [0012](0012-gate-based-adaptive-throttle.md) | Gate-based adaptive throttle for `ChessComClient` | Accepted, 2026-04-01 |
| [0013](0013-job-log-sink-survives-write-failures.md) | A job's log sink degrades and retries rather than switching off | Accepted, 2026-06-26 |
| [0014](0014-accept-both-database-url-forms.md) | Accept both database URL forms, lift credentials out of either | Accepted, 2026-08-13 |
| [0015](0015-server-read-idle-reaper.md) | One global read-idle timeout; live follows are reaped by it | Accepted, 2026-07-10 |
| [0016](0016-identity-is-the-id-names-are-observations.md) | Identity is the id; a name is an observation over time | Accepted, 2026-09-06 |
| [0017](0017-what-earns-a-history-table.md) | What earns a history table | Accepted, 2026-09-06 |
