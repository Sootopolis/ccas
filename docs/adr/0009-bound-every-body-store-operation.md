# Bound every `BodyStore` operation in time

**Status:** Accepted, 2026-08-14 (#211). Amended by #222 (attempt budget). Extends
[0008](0008-body-store-outside-postgres.md).

## Context

[0008](0008-body-store-outside-postgres.md) established that a store *outage* takes the cache
offline and never the app. Waiting on the store is never obligatory, so a store that is merely
*slow* has to degrade the same way — an unbounded read holds a fiber, and the caller could have
refetched from Chess.com in less time.

## Decision

A `Deadlines` decorator between the backend and `HealthLogging` applies
`body-store.{read,write}-timeout-ms` (defaults 5s / 10s, in code). Five things about it are
load-bearing:

1. **`.disconnect` is mandatory.** A plain `.timeout` waits for the inner effect's *interruption* to
   complete, and neither a `UrlConnectionHttpClient` socket read nor a stalled network-mount
   `Files.readAllBytes` honours `Thread.interrupt`. Measured: a 300ms budget on a 3000ms operation
   returned at 3020ms without it, 312ms with. `attemptBlockingInterrupt` therefore buys nothing on
   the `s3` path, whatever `S3BodyStore`'s comment implies.
2. **The transport needs its own ceiling.** `.disconnect` frees the caller but leaves the attempt
   running, so without `S3Timeouts` a stall becomes a leaked blocking-pool thread per read. The
   SDK's defaults are `DEFAULT_SOCKET_READ_TIMEOUT = 30s`, `DEFAULT_CONNECTION_TIMEOUT = 2s` and no
   API-call ceiling at all. `apiCallAttemptTimeout` and `apiCallTimeout` are *derived* from the
   widest accessor deadline rather than exposed as knobs, which is what makes an incoherent budget
   unconfigurable.
3. **The attempt budget is the whole budget (#222).** `apiCallAttemptTimeout` used to track the
   *socket* timeout — 5s inside a 10s write budget — making every put slower than it
   **unsatisfiable**: `ApiCallAttemptTimeoutException` is retryable, so a large upload was cut at 5s,
   restarted **from byte zero**, and cut again at 10s, at twice the uplink bytes, exactly when the
   link was the scarce resource. Observed 2026-08-15 as four `BodyStore write exceeded its 10000ms
   deadline` WARNs during a Recruitment archive fan-out (0.6–2.4 MB bodies, 20-way concurrent), with
   R2 healthy throughout — one object key had taken 4,022 puts at up to 8/s over the preceding 26
   minutes without a failure. Equal budgets also make the retry self-limiting, since `apiCallTimeout`
   is an absolute ceiling across attempts, so the SDK retry strategy stays at its default. `socket`
   is not the lever to reach for instead: it bounds inactivity on a socket *read*, not an upload,
   since the SDK streams a put with `setFixedLengthStreamingMode`.
4. **Do not shorten `apiCallTimeout` below the accessor deadline** to "let the SDK's error through".
   Tried during #222 and reverted. It surfaces only `ApiCallTimeoutException`, which carries no
   status code and leaves `numAttempts` unset, displacing `BodyStoreTimeoutException`, which names
   the operation, budget and byte count. Errors that *do* carry a status were never at risk: a
   429/503 exhausting LEGACY's four attempts costs ≤3.5s, well inside either budget.
5. **`Deadlines` must nest *inside* `HealthLogging`.** `HealthLogging.track` observes with `tapBoth`,
   which does not fire on interruption, so with the order inverted a store slow enough to blow every
   deadline never flips the degraded flag and the operator loses the one WARN. `TestBodyStore` pins
   both orders; nothing else would catch an inversion.

**Defaults are deliberately loose.** Set the read budget below the store's true p99 and the body
cache becomes an **amplifier** of Chess.com load — each bypassed read costs an unconditional GET plus
gate wait plus EMA delay, spending the scarce resource to save the abundant one. Tighten from
`client_stats.cache_unserved` and the store's own latency distribution, not from argument, and never
from `ChessComClient`'s throttle state: those are different failure domains, and `targetDelay` peaks
exactly when both paths are already slow. Cross-referencing the two is legitimate for *diagnosis*
(R2 slow while Chess.com is fine implicates R2; everything slow implicates our egress) and belongs in
`HealthLogging`. The rule is that coupling which changes *behaviour* across failure domains is
forbidden; coupling which changes *diagnosis* is free.

## Consequences

- **The transition log is a lower bound on failures, not a count.** `degraded` is one `Ref[Boolean]`
  mutated with `getAndSet`, and puts run concurrently, so only a failure observing the flag *clear*
  emits a WARN, while small puts landing between large ones keep flipping it back. In the #222
  incident, four WARN/INFO pairs in seven seconds meant at least six failed writes, and each
  "recovered" was a different, smaller put succeeding rather than the store healing. The design is
  right — it stops an outage emitting thousands of identical lines — but read it as "the cache went
  off at least once here", never as an outage timeline.
- **Known leak.** A timed-out `put` may still land its object while `putOrSkip` returns `false` and
  the caller correctly skips the pointer row. `ApiResponseBody.deleteOrphanRows` enumerates pointer
  *rows*, so that object is invisible to every sweep, permanently — it self-heals only if the
  identical body is fetched again. The same leak already existed on the delete side, since
  `deleteOrphans` drops the pointer row *before* best-effort-deleting the object, so bounding
  `delete` raises the rate of a pre-existing leak rather than introducing one. Accepted rather than
  reconciled: a sweep able to see these would have to enumerate the bucket, which content-addressing
  exists to avoid. Since the retention sweep became a forked fiber (ADR 0007), shutdown interrupts it
  mid-pass, so every restart during a sweep leaks the rest of that pass — routine, not exceptional,
  which promotes the lifecycle rule below from mitigation to requirement.
- **Mitigate with an age-based bucket lifecycle rule, not a reference-aware one** — S3/R2 cannot see
  the pointer table. The longest retention window (`cache_retention_days`,
  `fetch_failure_retention_days`) is the floor, not a safe setting: **the rule ages objects, retention
  ages rows, and the two are not related.** `touch` bumps `fetched_at` on every 304, while
  `putOrSkip` rewrites the object only on a 200 — so an entry that revalidates forever, which is
  exactly the immutable past-month archive the window exists to keep, holds a young row over an
  object of unbounded age. No fixed expiry avoids deleting some referenced objects; the setting only
  decides how often. Pick generously (180 days against a 60-day window) and read the cost below to
  see why the fetch-failure side is what forces the margin. Too low is degrading, not
  corrupting, but the two object kinds degrade differently: an early-expired *cache* body reads back
  as `BodyRead.Missing`, which invalidates and refetches (one wasted request), while an early-expired
  *`api_fetch_failure`* body has no refetch path — `selectRecent` renders the audit row without it and
  the evidence is gone. Input to #199. Scope the rule to a key prefix as soon as the bucket holds
  anything but `BodyStore` objects: a bucket-wide "delete after N days" is indiscriminate about
  object kinds nobody had thought of yet when it was written.
- Bodies are stored **uncompressed** while fetched gzipped (measured 4.6–4.9× on real archives), so
  the write path spends several times the read path's bytes on the narrower half of an asymmetric
  link. The back-pressure and observability half of that is #223.

