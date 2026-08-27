# The pacing EMA measures the HTTP exchange and nothing else

**Status:** Accepted, 2026-08-14 (#216). Refines [0012](0012-gate-based-adaptive-throttle.md).

## Context

`ChessComClient` spaces every outgoing request at `responseTimeEma / currentMax`. Anything folded
into that average therefore becomes artificial delay on the *next* Chess.com request.

`rawGet` does more than the network exchange: it also upserts `api_response_cache`, puts the body
into the `BodyStore`, and on failure writes an `api_fetch_failure` row. Timing the whole of it let a
slow object store or a cold Neon compute throttle Chess.com — the control action was applied to the
healthy domain, and the EMA stopped being usable as the "is Chess.com slow right now" signal that
separates a degraded R2 from degraded egress.

## Decision

Time `batchedClient(request)` alone. `client.batched` materialises the body before resuming, so that
single call is the whole network read and the `response.body.asString` downstream is in-memory.

Thread the duration out of `rawGet` on `ChessComClient.TimedFetch` rather than measuring around the
call, because `rawGet`'s own span still includes the storage writes.

Do **not** "simplify" this by moving the EMA update next to the exchange it now measures. That would
start sampling error responses, and cheap 404s are routine on the history crawl, so they would pull
the EMA down and speed us up toward the 429 threshold. `.timed` short-circuits on failure, so only
an attempt whose whole `rawGet` succeeded is recorded; a 404 or 429 feeds neither series.

## Consequences

- **`client_stats.latency_*` changes meaning at this commit.** It is read as API latency and follows
  the EMA, so rows before the change include the storage write and rows after it do not. Nothing in
  the table marks the boundary — a marker column would be carried forever to explain a one-off
  correction. Date it off the deploy, and distrust any latency regression that straddles it.
- **`client_stats.active_ms` deliberately keeps the wider `rawGet` window.** An active slot *is*
  held across the cache write, and narrowing it would understate utilisation.
- **A genuine 0 ms sample is now reachable**, because `Duration.toMillis` truncates. So "have we
  sampled yet" is `lastEmaSampleAt`, never `responseTimeEma > 0`. Both `emaDelay` and
  `updateResponseTimeEma` gate on the timestamp, so an EMA of 0 falls through to the
  `min-request-delay-ms` floor instead of disabling pacing. Full recovery resets the two fields
  together, which is what keeps that flag honest.
