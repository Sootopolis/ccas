# Cap API fan-out at the DB pool, not just the request gate

**Status:** Accepted, 2026-06-23 (#91).

## Context

Apps fan out API-bound work with `ZIO.foreachPar`. Two shared resources bound how wide that may
safely go, and they are not the same resource:

- the **Chess.com request gate** in `ChessComClient`, whose `maxPermits` bounds in-flight requests;
- the **Hikari connection pool**, which every fiber touches between requests to persist what it
  fetched.

The original cap considered only the first, at `2 * maxPermits`. The over-provisioning is
deliberate: a fiber that has cleared the gate spends real time on DB writes and JSON decode before
it needs the gate again, so a cap equal to `maxPermits` leaves the gate idle, while a much larger
one queues hundreds of fibers behind it for no gain (commit `6706d5ef`).

That is correct as a gate rule and wrong as a system rule. With `maxPermits` 16 the gate ceiling is
32 against a pool of 20, so a DB-phase burst could check out every connection at once. Health and
readiness probes, and any concurrent ad-hoc query, then queue on Hikari's `connectionTimeout` — the
app starves itself, and the symptom appears in the probe rather than in the fan-out that caused it.

## Decision

Take the smaller of the two ceilings: `min(2 * maxPermits, dbPool - PoolReserve)`, with
`PoolReserve = 2` connections withheld so a burst can never take the whole pool.

Subtract the reserve **only when `dbPool > 2 * PoolReserve`**. Applied blindly it wrecks a small
pool: the size-3 test pool would become `3 - 2 = 1`, silently serialising fan-out that used to run
three wide. The guard's invariant is that fan-out always keeps at least half the pool, so
concurrency can never collapse and only genuinely roomy pools pay the margin.

The arithmetic lives in `ApiConcurrency.cappedFor`, which is pure and takes the pool size as a
parameter, so the rule is testable without `ConfigFactory`. `TestApiConcurrency` holds the worked
cases — they are asserted there rather than tabulated in a comment, so they cannot drift.

## Consequences

- Prod (`maxPermits` 16, pool 20) caps at 18 rather than 32. This is the case #91 is about.
- At `maxPermits` 8 against pool 20 the gate ceiling (16) still wins; the reserve does not bite.
- The DB ceiling is slightly **non-monotonic** at the guard boundary — pool 4 gives 4, pool 5 gives
  3, pool 6 gives 4. That one-step dip is the price of pinning the size-3 test pool to full width.
  It cannot bite a real deployment, where the pool is 20.
- `dbMaxPoolSize` reads `ConfigFactory.load()` directly instead of asking a live `PostgresClient`,
  matching the shortcut `Tables` already uses for cache config rather than threading a client
  through every `fiberCap` call site for what is a static ceiling. Two consequences follow, both
  accepted: a `PostgresClient.live(prefix = …)` built under a non-default prefix is invisible here,
  and an absent config path yields `Int.MaxValue`. Both simply drop the DB ceiling, reverting to
  `2 * maxPermits`. Every current caller uses the default prefix, so the first is latent; threading
  the resolved prefix is the upgrade trigger if a second prefix is ever introduced.
