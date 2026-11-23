# Stay with Magnum for SQL access

**Status:** Accepted, 2026-03-14. Recorded here from #206. Amended by #536 (Magnum 2.0.0).

## Context

CCAS reaches PostgreSQL through Magnum: raw SQL in `sql""` interpolators, with rows read by derived `DbCodec`s. We looked at zio-protoquill because Magnum statements did not compose as ZIO effects inside one transaction.

`PostgresClient.withTransaction` (8ad9968, 2026-03-14) solved that problem without Quill. It runs a multi-statement ZIO effect on one connection, commits or rolls back once, and retries the whole transaction, not single statements.

What is still awkward about Magnum is minor: SQL lives in strings, enums are converted by hand, and conditional queries are clumsy. Two of its pitfalls were listed under SQL in `CLAUDE.md` until Magnum 2.0.0 removed them.

## Decision

Stay with Magnum. Do not migrate to zio-protoquill.

At the time, a migration meant rewriting more than 50 query methods across 16 entities. It would also have brought a compile two to three times slower, macro errors that are hard to read, and a dependency maintained more slowly than Magnum. None of that was worth it to fix the minor problems above.

## Consequences

- SQL stays as raw strings, and the tests against Postgres are what check it. Nothing checks queries at compile time.
- The two Magnum pitfalls once listed in `CLAUDE.md`, a codec meeting `null` and a bare enum case in `sql""`, went with the move to Magnum 2.0.0 (#536). What the move cost is in [0002](0002-dependency-decisions-2026-08.md).
- The risk of staying is that our version goes stale, not that the library is a poor fit. Upstream works on the 2.0 line, which we are on since #536.

## Revisit when

- SQL grows a lot more complex: about 50 tables, real reporting queries, or more than one developer.
- SQL bugs start reaching production that the tests miss. Checking queries at compile time pays off only once there is a real defect rate for it to reduce.

If we do move, look first at zio-jdbc, which gives ZIO-native raw SQL and is lighter than Quill, before zio-protoquill.
