# Recover renamed usernames and club slugs instead of treating a 404 as death

**Status:** Accepted, 2026-05-07 (#23).

## Context

Chess.com permits handle changes. When a player or club is renamed, the old username or slug starts
404ing while the entity itself is still reachable under a new name. Treating the 404 as permanent
loses the entity and its history; treating every 404 as a rename would burn requests on handles that
genuinely no longer exist.

A 404 is also not stable over time: `"X not found"` bodies are timeline-unstable, and a username or
club slug can flip 404 → 200 when someone later registers that handle.

## Decision

`UsernameRenameResolver` and `ClubSlugRenameResolver` (`ccas.analysis.apps`) recover the current
canonical name, in tiers, cheapest first. `ZIO.collectFirst` runs each tier only when the previous
one returned `None`.

- **Tier A — pure DB lookup.** `Player.selectByUsername` plus
  `PlayerSnapshot.selectLatestPlayerIdByUsername` for players; `Club.selectId` for clubs. Succeeds
  whenever our DB already learned the rename by some other path.
- **Tier B — the board-endpoint trick.** `PlayerMatchRef.findOrInfer` → `ApiMatchBoard`, eliminating
  the opposing side's known canonical name; or `Club.slugFromMatchRef` → the match's team URL.
- **Tier C — admin clubs (clubs only).** Closes the gap for a club that has never played a match,
  where Tier B is a no-op. Loads stored `ClubAdmin` rows for the `clubIdHint`, fetches each
  non-tombstoned admin's `/pub/player/{u}/clubs`, filters out slugs we already know via
  `Club.selectExistingSlugs` (those would have surfaced in Tier A), and returns the first slug whose
  verified `ApiClub.clubId` matches the hint. Bounded by the admin count, short-circuits on first
  hit. `ClubDataApp.refreshClub` passes `Some(club.clubId)`, so Tier C is reachable from the typical
  call site.

A verification fetch confirms the `playerId` / `clubId` matches the hint before the new name is
returned.

Callers wire recovery in via the `withPlayerRenameRecovery` and `withClubSlugRenameRecovery`
extension methods on any 404-prone effect, or `UsernameRenameResolver.fetchOrRecover` for the common
"fetch `ApiPlayer` with rename fallback" shape.

**Resolver internals never replace the caller's original 404** with a recovery-internal error. Tier A
SQL exceptions and Tier B/C HTTP errors are debug-logged and fall through to "no rename inferred".

## Consequences

- A player or club whose canonical name cannot be discovered — no match refs, no snapshot history, no
  admin signal — is tombstoned with a sentinel `_stale_<id>` (`PlayerUpdater.archiveAndUpdate` for
  recycled-handle cases, `Club.resolveStaleSlug` for fresh slug-discovery failures) so the UNIQUE
  constraint slot is freed.
- Tombstoned rows are skipped at iteration sites (for example `HistorySeeding.seedFromMemberMatches`)
  and rendered as `<unknown player #<id>>` / `<unknown club #<id>>` through `Player.displayUsername`
  and `Club.displayName`.
- The collision-safety of the `_stale_<id>` format is tracked in
  [Sootopolis/ccas#21](https://github.com/Sootopolis/ccas/issues/21).
- High 404 counts on cancelled matches remain expected noise and are not a rename signal.
