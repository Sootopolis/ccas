# Gate-based adaptive throttle for `ChessComClient`

**Status:** Accepted, 2026-04-01, with the recovery ladder following on 2026-04-05. Supersedes
[0001](0001-adaptive-rate-limiting.md). Refined by
[0006](0006-pacing-ema-measures-the-http-exchange-only.md) (#216).

## Context

[0001](0001-adaptive-rate-limiting.md) chose a binary throttle: N-permit semaphore normally, a
`Semaphore(1)` mutex after a 429, back to parallel after a fixed cooldown. Its own "Cons" line named
the flaw — no intermediate states. In practice that meant a single 429 collapsed the crawl to fully
sequential and then jumped straight back to full width, which either re-triggered the 429 or left
throughput on the floor for the whole cooldown. A `Semaphore` also cannot be resized, so there was no
way to express "half speed".

## Decision

Replace the semaphore pair with a gate the client controls, and give throttle-down and recovery
independent, gradual mechanisms.

**Gate-based admission control.** A single-permit gate serialises *admission*, not execution. Before
each request the gate checks in-flight requests (`activeRef`) against `currentMax`; if there is no
slot it polls until one opens, then atomically increments the count before releasing the gate. This
enforces the limit without a semaphore, so a throttle-down takes effect immediately for every request
that has not yet entered the gate. The gate wait is interruptible, so pending requests cancel
promptly on shutdown.

**EMA-based pacing.** An exponential moving average of response times staggers outgoing requests so
the permit budget is used without bursting; when permits are reduced, the per-request delay grows
proportionally. `min-request-delay-ms` is a hard floor that prevents bursts when responses are
unusually fast. The EMA decays on wall-clock time — each sample shifts it by `1 - exp(-dt / emaTauMs)`
— so one slow outlier decays out in real time rather than over a fixed number of samples, which
matters for small-N sequential workloads. What it does and does not measure is
[0006](0006-pacing-ema-measures-the-http-exchange-only.md).

**Failure-window throttle-down.** A rolling window of outcomes drives the decision. HTTP 429 counts as
a failure; non-rate-limit responses (403 non-Cloudflare, 404, 500) count as successes. Connection
errors are retried but feed neither side — reducing permits does not fix a broken network. When the
failure rate exceeds `failureThreshold` over at least `minSampleSize` outcomes, `currentMax` drops to
1 and the gate enforces it at once.

**Cloudflare challenges bypass the window.** A CF 403 hard-throttles to 1 permit immediately,
independent of the failure rate, because it is a categorical signal rather than a noisy one.

**Generation-gated recovery.** After a cooldown, a background fiber walks `currentMax` up through the
configured `recoveryTiers`, one step per cycle, holding if failures persist. Each tier must be
observed for `min-tier-observation-seconds` before promotion is evaluated — without it, high
concurrency fills the outcome window in moments and the ladder is climbed before the tier has been
tested. `coolingDown` stays true throughout the ladder so `recordOutcome` cannot trigger a second
throttle-down mid-recovery; it clears only when permits reach `maxPermits`. A generation counter
ensures only the most recent throttle-down drives recovery, so a stale fiber cannot interfere.
Recovery fibers are scoped to the client's lifetime and interrupted when the layer is torn down.

**Separate retry schedules,** each with its own budget: 429 (exponential backoff, `max-429-retries`),
Cloudflare 403 (fixed delay, `max-cf-retries`), transient connection errors (exponential backoff,
`max-connection-retries`). Non-Cloudflare 403 and 404 are permanent and never retried.

**Cumulative stats flushing.** A daemon fiber upserts one `client_stats` row per session every
`stats-flush-interval-seconds`, overwriting the previous snapshot with cumulative totals. The throttle
configuration is inserted once into `client_config` on first flush and referenced by FK. A final flush
runs in the scope finaliser, so stats survive a non-graceful shutdown with at most one interval's
loss.

Configuration is read from `application.conf` under the `chess-com-client` prefix by
`ChessComClient.live`.

## Consequences

- Throttle-down is still all-the-way-to-1, but recovery is gradual, so the sawtooth of 0001 is gone.
- `getAll[T](urls)` batches via `ZIO.foreachPar` capped at `maxPermits`. Network-bound fibers already
  bottleneck at the gate, so the cap is really there to bound cache-warm fan-outs against the Hikari
  pool — see [0004](0004-api-fan-out-concurrency-cap.md).
- The `followRedirects(3)` aspect's error handler returns 304 responses as-is rather than failing on
  the missing `Location` header, which is what lets the conditional-GET path in
  [0007](0007-response-caching-in-postgres.md) work at all.
- Every failed attempt is recorded per-attempt in `api_fetch_failure`, with response bodies deduped
  through `api_response_body`.
- `CCAS_CONTACT_EMAIL` is required for the `User-Agent` header; the client will not build without it.
