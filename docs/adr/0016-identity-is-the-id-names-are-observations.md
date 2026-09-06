# Identity is the id; a name is an observation over time

**Status:** Accepted, 2026-09-06 (#180). Replaces the tombstone consequence of
[0010](0010-rename-recovery-for-usernames-and-club-slugs.md) once the migration below lands; 0010's
tier design stays live and this ADR depends on it.

## Context

Chess.com gives every club and player a stable numeric id and a mutable handle. Handles can be
renamed, and a freed handle can later be registered by someone else. The schema says otherwise —
`club` carries `UNIQUE INDEX club_slug_key ON club (slug)`, `player` carries `UNIQUE (username)`.

The deeper error is not the index. **Holding a name is a relationship with a lifetime, and it is
stored as a mandatory attribute.** A `NOT NULL` column cannot express "this club currently holds no
name we know of", so a sentinel had to be invented to occupy the slot: `_stale_<id>` tombstones
(`Club.resolveStaleSlug`, `PlayerUpdater.archiveAndUpdate`), `Club.upsertResolvingSlugConflict` as a
read-then-repair transaction to dodge the resulting collisions, sentinel match sites scattered through
display and query code including a hand-copied SQL regex in `ManagedClub` that restates
`Club.stalePattern`, and
[#21](https://github.com/Sootopolis/ccas/issues/21) asking whether that format is even collision-safe.
[#176](https://github.com/Sootopolis/ccas/issues/176) is the same cause at the CLI: a slug we have
seen before does not resolve, because the row was overwritten with the newer one.

**The migration surface is small, and that is measured rather than hoped.** Every downstream table
already keys on the id, and nothing joins on `club.slug`. The bare names stored elsewhere are
`player_snapshot.username` (addressed below), `unresolved_match_club.slug` (the same observation this
ADR is about) and `player_tournament_ref.tournament_slug`, which is out of scope.

[#186](https://github.com/Sootopolis/ccas/issues/186) fixed this for `current_club` by carrying the
id, and [#180](https://github.com/Sootopolis/ccas/issues/180) then deferred a slug-history table as
"non-load-bearing" on that strength. The id only helps where the caller already holds one: a freshly
typed `--club <slug>`, `--all` over a renamed club, and every recycled handle are untouched. The
deferral generalised from one case to all cases.

## Decision

**An entity's identity is its Chess.com id. Holding a name is a relationship with a window, stored as
its own row, never as an attribute of the entity.**

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE club_name (
  club_id  BIGINT      NOT NULL REFERENCES club (club_id) ON DELETE RESTRICT,
  slug     TEXT        NOT NULL,
  since    TIMESTAMPTZ NOT NULL,
  until    TIMESTAMPTZ,
  PRIMARY KEY (club_id, since),
  CONSTRAINT club_name_window CHECK (until IS NULL OR until > since),
  CONSTRAINT club_name_no_overlap
    EXCLUDE USING gist (club_id WITH =, tstzrange(since, until) WITH &&)
);
CREATE UNIQUE INDEX club_name_current ON club_name (slug) WHERE until IS NULL;
```

The range is an **expression inside the constraint, not a stored column**, so Magnum needs no range
codec. An open row is unbounded above, so two open rows for one club overlap and are rejected; the
partial unique index adds that one name is never currently held by two clubs. Both are *current-slice*
invariants — violating either makes a live lookup return a wrong answer, so the database enforces
them. The same constraint on `slug` across clubs is deliberately **not** applied: it would forbid
historical overlap, and our observation windows lag reality (a late rename discovery, or a body served
from the response cache, [0007](0007-response-caching-in-postgres.md)), so it would reject legitimate
writes. `player_name` mirrors this shape exactly.

**Lookup is two-step, and ships with no index over historical names.** Ask
`slug = $1 AND until IS NULL` first — the constraint's own index, and the hot path — then `slug = $1`
only on a miss; the two cases need different handling anyway. Today's one historical-name lookup,
`idx_player_snapshot_username`, has 373 lifetime scans against 6.5M on `player_username_unique`, so a
full index would earn little. Read `pg_stat_user_indexes` after the first sweep and add it if the cold
path turns out to fire in a loop.

### The window records knowledge, not truth

`[since, until)` is the span over which our observations are consistent with this name holding.
`tstzrange` is half-open, so `until` excludes its own instant — correct, because `until` is stamped by
the observation that found the name gone. `until IS NULL` means "the most recent observation still
stands", not "verified now"; `club.fetched_at` already records when that was last refreshed.

**Both bounds are discovery times, deliberately.** Chess.com exposes only current state, so the true
transition is unknowable and any stored value is a bound. Stamping the later bound keeps history
append-only, so a report over a past window returns the same answer when re-run — an estimated earlier
timestamp would silently break that. It also matches how `player_snapshot.since` is already stamped.

**Gaps are meaningful and permitted.** A rename closes the old row and opens the new one at the same
instant, so the ordinary path leaves none. A gap arises exactly when a name is confirmed gone and no
replacement is found — 0010's tiers exhausted — which is today's tombstone case, now carrying dates.
Forcing contiguity would invent a transition we never observed.

### What earns a history table

The rule that decides which columns get one, and why nothing else about a club or player needs it, is
[0017](0017-what-earns-a-history-table.md). Only the name was in the wrong bucket.

## Consequences

- **`club` converges on the shape `player` already has**: an id-keyed core, a denormalised copy of the
  current state for convenience, and the history in its own table. `club.slug` and `player.username`
  become display caches — they lose their unique indexes and keep their ~220 field reads, but nothing
  may *resolve* through them again. A stale cache must only ever cause a display glitch.
- **`selectBySlug` splits.** A recycled handle means one name maps to several clubs across time, so
  callers must ask either "who holds this now" or "who ever held this".
- **The server can reconcile id against name instead of ignoring it.**
  `ClubResolution.resolve(clubId, slug)` drops the slug today; with `club_name`, a disagreement becomes
  a precise answer — the id's name changed, or the name moved to another club — which catches a stale
  client cache rather than silently running the wrong job.
- **The resolved id travels with the job, and the fetch verifies it.** `MembershipApp` fetches via
  `fetchOrRecover` with no id hint, so a name that has since moved to another club silently retargets
  the job and tombstones the original (`ClubDataApp` already passes a hint and is safe). Once
  submission resolves to an id, the fetch must confirm `ApiClub.clubId` matches it and abort on a
  mismatch; resolving by id without that check leaves the hole open.
- **`player_snapshot.username` becomes redundant and should be dropped.** Both readers are better
  served by `player_name`: 0010's Tier A wants a complete indexed name→id mapping, and
  `MembershipReport` wants windows rather than change points. The cost is real: a pure rename then
  writes no snapshot row, so the report merges two sources.
- **Migration is additive first, destructive last.** There is no migration framework, so each step is
  hand-written against every database:
  1. add `club_name` and backfill from `club.slug`;
  2. route lookups through it *and pass the resolved club down*, so `HistoryApp`, `StatsApp` and
     `ClubDataApp` stop re-resolving a slug the gate already resolved;
  3. drop the unique index on `club.slug`;
  4. delete the tombstone machinery — Scala helpers *and* `ManagedClub`'s SQL regex — converting
     `_stale_*` rows to "no current name";
  5. repeat for `player`, which also retires `player_snapshot.username`.

  `CREATE EXTENSION btree_gist` is run by hand with the other statements rather than from
  `Tables.ensureTablesOnInit`, so privileged DDL stays out of the boot path. (Verified available on
  Neon; `ccas_owner` is a member of `neon_superuser`.)
- **Backfilled rows start at the migration instant.** No first-observation timestamp exists for names
  we already hold. Understating the window is honest; inventing an earlier bound is not.
- **Measured 2026-09-06, and the reason to do it now:** 11,414 clubs with **0** tombstoned rows; 84,638
  players with **1**; 89 players already carry more than one username in `player_snapshot`, so the
  player backfill has real history to seed from. The destructive step is a one-row conversion today and
  grows with every month of running.
- **Two ADRs go stale as this lands, and neither is superseded yet.** Tombstones are what the code does
  until step 3, so 0010 stays `Accepted` until that commit, then is marked superseded in part with its
  citations updated (`CLAUDE.md` and four files under `ccas.analysis.apps` / `ccas.utils.client`).
  [0011](0011-cli-locality-and-the-current-club-pointer.md)'s "a slug-only target still resolves by
  exact slug, so `ClubSlugRenameResolver` stays unreachable from that path" stops being true at step 2.
- #176 closes at the routing step. #180's slice 3 (opt-in upstream reach) stays worth building but is
  demoted: it serves clubs never seen under any name, not renames. #21 closes with the tombstones.
  `unresolved_match_club` becomes a candidate to fold in — a separate decision.
- **Not fixed:** a rename we have never observed still costs one upstream fetch.
- **The general rule this instance stands for:** never key a table on a mutable external label. When an
  upstream system owns both an id and a name, the id is the identity and the name is history.
