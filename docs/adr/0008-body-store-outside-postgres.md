# Keep response bodies out of Postgres, in a content-addressed store

**Status:** Accepted, 2026-08-07 (#191, #200). Amended by #211 (deadlines), #215 (three-valued read), #222 (attempt budget).

## Context

Response *metadata* is small, indexed and queried; response *bodies* are large, opaque and only ever
fetched whole by hash. Keeping both in Postgres made the database grow with traffic for no
query-side benefit.

## Decision

Bodies live in a content-addressed `BodyStore` (`ccas.utils.client`), keyed by the same hex SHA-256
that `api_response_body` records — so `put` is idempotent and dedup is preserved for free. Two
backends, selected by `body-store.backend`:

- `fs` (default) — `FsBodyStore`, objects at `root/<hash[:2]>/<hash>`, written temp-then-rename so a
  concurrent reader never sees a half-written body.
- `s3` — `S3BodyStore`, AWS SDK v2 sync with `UrlConnectionHttpClient`, path-style, pointed at
  Cloudflare R2 via `CCAS_R2_*`.

Only the blobs move. The metadata index, the within-`max-age` short-circuit and ETag/304
revalidation all stay in Postgres — see [0007](0007-response-caching-in-postgres.md).

**The fs root defaults in code** (`BodyStore.resolveFsRoot`) to `${XDG_CACHE_HOME:-$HOME/.cache}/ccas/bodies`,
not as a HOCON literal: a relative literal is CWD-dependent and dropped runtime blobs into whatever
directory the process happened to start in. The XDG resolution is inlined rather than reusing
`ccas.cli.XdgPaths`, because `ccas.utils.client` must not depend on `ccas.cli` — keep the two in
step. It returns `Either` and blank-checks every fallback, so an unresolvable root fails the layer
with an actionable message: `Paths.get("")` is the CWD, and a null `user.home` interpolates to a
*relative* directory named `null`, either of which would silently reinstate the CWD-dependence.

### The degradation invariant

**The body cache is not source of truth, so a store outage takes the cache offline, never the app**
— and since waiting on it is never obligatory, a store that is merely *slow* must degrade the same
way (#211).

The trait is honest about I/O (`Task`), but the only companion accessors are the error-degrading
ones: `BodyStore.read` and `BodyStore.putOrSkip` (error → `false`). There is deliberately no raw
`get` / `put` accessor to reach for by mistake; `delete` stays raw because every call site is
already a best-effort `.ignore`.

`ApiResponseBody.putBody` therefore returns `Option[String]`, and a `None` must skip the **whole**
write: `ApiResponseCache.upsertWithBody` returns `Option[ApiResponseBodyId]` and persists neither the
pointer nor the cache row, because a pointer with no object behind it is a cache entry that can only
ever produce a refetch. Net effect of an outage: requests keep succeeding uncached, and the cache
self-heals on the first successful put. `ApiFetchFailure.insert` degrades to a body-less audit row;
`normalizeCfBodies` skips its pre-warm rather than failing boot. #199's budget guard is this same
mechanism.

### Put before the transaction, never inside it

`ApiResponseBody.putBody` must run *before* the JDBC transaction that writes the pointer, cache or
failure row. The `PutObject` is a network round-trip, and running it inside `withTransaction` would
pin a pooled Postgres connection idle-in-transaction for that round-trip on the hot write path.

Put-then-insert is also the safe order for readers: a committed pointer always has bytes behind it,
where the reverse would let a reader see a pointer with no object. A put whose transaction later
rolls back leaves a harmless dangling object — re-putting is idempotent, and `deleteOrphans` sweeps
it.

The SHA-256 is taken over the String's UTF-8 bytes, so the store key and the stored bytes stay
consistent.

### A missing object and an unreachable store are not the same failure (#215)

`BodyStore.read` returns a three-valued `BodyRead` — `Found` / `Missing` / `Unavailable` — because
the two non-`Found` cases demand different repairs even though a refetch heals both.

`Missing` (pruned pointer row, absent object) means the `api_response_cache` row is a lie, so
`ChessComClient.loadAndDecode` drops it. `Unavailable` (store error *or* deadline breach) means the
row is still accurate and still holds the ETag and `max-age`, so it is **kept**: invalidating would
spend a transient outage's worth of validators permanently, leaving every URL touched during it to
pay a full unconditional GET after recovery instead of a 304.

Either way the refetch is unconditional — a conditional GET could return 304 and leave us with
metadata and still no body — and the refetch's own `upsertWithBody` no-ops while the store is down,
so the preserved row self-heals on the first successful put. `BodyRead.toOption` exists for callers
where the distinction genuinely does not matter (`ApiFetchFailure.selectRecent`'s audit display);
cache code pattern-matches.

### Health reporting is separate from degradation policy

`BodyStore.live` wraps the chosen backend in `Deadlines`, then `HealthLogging`. The latter tracks a
degraded flag and logs only the *transitions* — one WARN on the first failure, one INFO on the next
success — then re-raises the error unchanged. The accessors' own per-operation failures log at DEBUG:
an outage fails every read on a fetch-heavy run, and a WARN each would bury the signal it exists to
raise.

Keep the split: `Deadlines` enforces the clock, `HealthLogging` observes, the accessors decide what
failure means. #199's budget guard goes **above** `HealthLogging`, not below it next to `Deadlines` —
a quota decline is a deliberate policy decision, and observed from underneath it would log "BodyStore
unavailable" on every skipped write.

Because a content-addressed store can only ever name a hash, the non-`Found` branches of
`loadAndDecode` log the URL, and the write path threads a `source` through `ApiResponseBody.putBody`
into `putOrSkip`'s DEBUG line. A breached write carries its byte count on `BodyStoreTimeoutException`
so the one WARN names the size.

## Consequences

- Operations are bounded in time by a `Deadlines` decorator; see
  [0009](0009-bound-every-body-store-operation.md) for the budgets, the transport ceiling and the
  object leak a breached write can strand.
