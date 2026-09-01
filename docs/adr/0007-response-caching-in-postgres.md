# Cache API responses in Postgres, keyed by URL

**Status:** Accepted, 2026-04-16. Body storage split out in [0008](0008-body-store-outside-postgres.md).

## Context

The history crawl and recruitment fan-outs re-request the same Chess.com URLs across runs. Chess.com
is the scarce resource; Postgres is not.

## Decision

Persist every successful fetch to `api_response_cache`, keyed by URL, with ETag / Last-Modified /
`Cache-Control: max-age` / Content-Type metadata and a pointer to the body.

`ChessComClient.getCacheable[T](url)` returns a `CacheableResult[T]` with four variants:

- `Fresh` — served from cache, no network call; the entry was within `max-age`.
- `Revalidated` — a conditional GET returned 304; `fetched_at` is refreshed.
- `IdenticalBody` — 200 OK whose body was byte-identical (SHA-256 dedup kept the same `body_id`).
- `Changed` — first fetch, or a real content change.

Four variants rather than one `Unchanged` because the distinction is observability: "we never asked
the server" is a different fact from "the server confirmed unchanged" and from "we asked and got the
same bytes back". The three hit variants share a sealed `Unchanged[T]` supertype so callers can
branch on "any cache hit" uniformly, and `foldZIO` / `unlessUnchangedDiscard` dispatch through
`final` overrides on it rather than a pattern match — that avoids an erased `case c: Changed[T
@unchecked]` cast and lets the compiler verify T end to end.

Each variant carries a lazy `getValue: Task[T]`, so a caller that only branches on `isUnchanged` pays
nothing beyond the cache-row lookup. `Changed` is eager, so decode errors on network responses
surface at fetch time rather than inside the caller. `get[T]` is a thin
`getCacheable[T](url).flatMap(_.getValue)`.

Current callers: `HistoryProcessing.refreshSingleMatch` (unchanged → bump `fetched_at`; changed →
full rework) and `HistorySeeding.seed{FromClubMatches,MatchesForPlayer,MatchesForPlayerAllClubs}`
(unchanged → skip the INSERT pipeline but still stamp `HistoryMemberQuery` where required).

### Wire-format details that are not optional

- **ETags are stored in wire format** (`"..."` / `W/"..."`) and echoed via
  `Header.Custom("If-None-Match", …)`. zio-http's `Header.ETag.parse` strips the quote delimiters,
  so an etag routed through the typed header renders unquoted and the origin will not match it. The
  render side is faithful; the loss is in the parse.
- **`Last-Modified` is read via a raw-header lookup** piped through `ccas.utils.HttpDate.parse`.
  Chess.com ships `Thursday, 16-Apr-2026 23:13:22 GMT+0000`, which matches none of the three forms
  in RFC 7231 §7.1.1.1, and zio-http's typed `Header.LastModified` silently rejects it. The parser
  tries Chess.com's shape first, then IMF-fixdate / RFC 850 / asctime. `If-Modified-Since` is always
  sent as IMF-fixdate regardless of what was received.
- **`Cache-Control: no-store` is not cached. `no-cache` is honoured** by persisting the entry with a
  cleared `max_age_seconds`, so every later request revalidates (RFC 7234 §5.2.2.2).
- **On a 304, `ApiResponseCache.touch` merges** refreshed values using `COALESCE` for the validators
  (an absent header preserves the stored value) and a `MaxAgeUpdate` ADT — `Preserve` / `Clear` /
  `Overwrite(n)` — covering the three wire-level distinctions: header absent, `no-cache`, `max-age=n`.

## Consequences

- Empirically (2026-04-17) Chess.com's origin ignores `If-Modified-Since` whatever the format, so the
  conditional-GET path is in practice ETag-only.
- Cache hits, revalidations and misses get dedicated counters
  (`client_stats.cache_hits` / `cache_revalidations` / `cache_misses`) so the `requests` series stays
  an honest measure of Chess.com load. The three HistoryApp skip sites also record per-run counters
  on `history_run` (`refresh_match_unchanged`, `seed_club_matches_unchanged`,
  `seed_player_matches_unchanged`), so the effect on downstream DB and decode work is visible
  separately from transport-level savings.
- **`client_stats.cache_unserved` is the reconciling term.** `cache_hits` and `cache_revalidations`
  are incremented on the metadata lookup, before the lazy `getValue` has read anything, so an entry
  counted as served can still cost a full round trip. Genuinely-served entries are
  `cache_hits + cache_revalidations - cache_unserved`, *provided each result's `getValue` is forced
  at most once* — nothing enforces that, since `getValue` is a plain re-runnable `Task`, so read the
  difference as an estimate rather than an identity. The counter is additive and monotonic on
  purpose: retracting `cache_hits` would change the meaning of an already-populated column and would
  rest on the same unenforced invariant while also breaking comparability across sessions.
- **Retention.** `Tables.retentionSweep` calls `ApiResponseCache.deleteBefore(now - retention)`,
  chained with `ApiResponseBody.deleteOrphans` in one transaction. The window is
  `cache_retention_days` from `app_setting` (default 60 days when the row is absent or unparseable).
  The same pass sweeps `api_fetch_failure` via `FetchFailureRetentionDays` (default 30) — without it
  that table grows unbounded, since every failed attempt writes a row plus a body, a 404 body embeds
  the requested slug so dedup never collapses distinct bogus slugs, and orphan bodies are pinned by
  `ON DELETE RESTRICT`. There is no HOCON or env mirror; change it with SQL, effective on the next
  daily sweep pass. The compiled default rose from 7 days to 60 once bodies left Postgres for the
  BodyStore: what an entry costs is now R2 storage plus a metadata row, and `touch` bumps
  `fetched_at` on every revalidation, so the window only governs entries nothing revisits.
- **The sweep is not boot work.** It lived in `Tables.ensureTables`, which runs inside
  `PostgresClient.live`'s init hook — so the HTTP port stayed closed until it finished. Its cost is
  a function of how much aged out since the last boot, which on a long-lived server is the whole
  backlog: the DB delete commits first, then one object delete per freed hash. At R2 round-trip
  latency that is minutes to tens of minutes of silent startup, and killing the process "fixed" it
  only because the committed delete left the next boot nothing to sweep — at the price of orphaning
  every not-yet-deleted object permanently (the pointer row is already gone, so no sweep can ever
  see it again). `CcasServer` now forks `retentionSweep` alongside `Server.serve`, and the object
  deletes run at `ApiResponseBody.ObjectDeleteParallelism`. It then repeats daily, so what a pass
  removes is roughly one day of expirations rather than everything since the last restart — the
  restart cliff was the boot cost's real cause, and forking alone would only have hidden it. Each
  pass re-reads `app_setting`, so a retention change lands within a day. One consequence worth
  knowing: a standalone app run (`RefApp` and friends, which wire `ensureTablesOnInit`) no longer
  sweeps at all — retention is now the server's job alone.
- Mid-flight races are tolerated: a `Fresh` / `Revalidated` result whose body was pruned by another
  process falls through to a recursive network refetch via `loadAndDecode`'s `None` branch or its
  `JsonDecodingException` recovery. Forking the sweep makes that the expected case rather than the
  theoretical one — it now overlaps the scheduler's own boot backlog (#212) instead of finishing
  before the port opened — so the price of a lost race is a wasted request, paid at whatever rate
  the sweep and the fetch path collide. That, not sweep throughput, is what caps
  `ObjectDeleteParallelism`.
- "Disable caching" would be a future `CacheMode` (a mode), not a `BodyStore` backend value (a
  location): a null store would still run the metadata path and revalidate-then-refetch every time.
