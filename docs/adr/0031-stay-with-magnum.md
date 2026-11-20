# Stay with Magnum for SQL access

**Status:** Accepted, 2026-03-14. Recorded here from #206.

## Context

CCAS reaches PostgreSQL through Magnum: raw SQL in `sql""` interpolators, with rows read by derived `DbCodec`s. We looked at zio-protoquill because Magnum statements did not compose as ZIO effects inside one transaction.

`PostgresClient.withTransaction` (8ad9968, 2026-03-14) solved that problem without Quill. It runs a multi-statement ZIO effect on one connection, commits or rolls back once, and retries the whole transaction, not single statements.

What is still awkward about Magnum is minor: SQL lives in strings, enums are converted by hand, and conditional queries are clumsy. Two of its pitfalls are listed under SQL in `CLAUDE.md`.

## Decision

Stay with Magnum. Do not migrate to zio-protoquill.

At the time, a migration meant rewriting more than 50 query methods across 16 entities. It would also have brought a compile two to three times slower, macro errors that are hard to read, and a dependency maintained more slowly than Magnum. None of that was worth it to fix the minor problems above.

## Consequences

- SQL stays as raw strings, and the tests against Postgres are what check it. Nothing checks queries at compile time.
- The two Magnum pitfalls in `CLAUDE.md` stay until Magnum 2.0 is stable. Its milestones fix both. The version we pin, why it is held, and what moving to 2.0 would cost are in [0002](0002-dependency-decisions-2026-08.md).
- Magnum's upstream work all happens on the 2.0 line, so the risk of staying is that our version goes stale, not that the library is a poor fit.

## Revisit when

- SQL grows a lot more complex: about 50 tables, real reporting queries, or more than one developer.
- SQL bugs start reaching production that the tests miss. Checking queries at compile time pays off only once there is a real defect rate for it to reduce.

If we do move, look first at zio-jdbc, which gives ZIO-native raw SQL and is lighter than Quill, before zio-protoquill.
