# What earns a history table

**Status:** Accepted, 2026-09-06 (#180). Generalises
[0016](0016-identity-is-the-id-names-are-observations.md), which is the instance that prompted it.

## Context

[0016](0016-identity-is-the-id-names-are-observations.md) gave clubs and players a name-history table
because storing only the latest name destroyed information the system needed. Left there, that reads
as licence to historise every mutable column — members counts, activity timestamps, display names —
which would add tables nothing reads and writes nobody consumes. The opposite reading, that history is
a luxury, is what produced the tombstones in the first place. A rule is needed that decides which.

## Decision

**The test is whether absence is representable.**

A **continuous attribute** always has some value — a player always has a status, a club always has a
member count. It has a start and no end, so its history is an append-only log keyed `(id, since)`,
with currency implied by the latest row. `player_snapshot` is exactly this and is correct as it
stands.

A **relationship** can be absent. Holding a name, or being a member of a club, is something that
starts, ends, and may not currently hold at all. It needs both ends, and a gap between rows carries
meaning. `club_member` already uses `since` / `until` for that reason, and `club_name` follows it.

Forcing a relationship into an attribute column is what created the `_stale_<id>` sentinel: a
`NOT NULL` column cannot say "holds nothing right now", so a placeholder had to occupy the slot.

Three other kinds need no history at all:

- **Derived aggregates** — `club.members_count` (the real history is `club_member`),
  `club.latest_match_at` (computed by `ClubDataApp` from our own match rows). Recompute; a second copy
  is a worse one.
- **Our own bookkeeping** — `club.fetched_at` records what *we* did, not what the club did.
- **Immutable upstream facts** — `club.created`, `player.joined`.

And one that is mutable but still needs nothing: **display-only state**. `club.name` changes upstream,
but nothing resolves by it and no invariant touches it, so it keeps being overwritten. Mutability
alone is not a reason to historise, and it is not a reason to split a table either.

## Consequences

- There is deliberately no `club_snapshot`. Club state history has no consumer, and the quantities
  anyone might ask for are derivable. If one appears, add `club_snapshot` mirroring the player side
  rather than splitting the club row into mutable and immutable halves — "immutable" is an
  observation about a column, not a reason to give it its own table.
- A relationship table carries the integrity cost that comes with two ends: overlap has to be
  excluded, and "no current row" has to be a legal state. 0016 holds the constraint shapes.
- The rule is about storage, not about reporting. A consumer that wants a timeline it cannot derive is
  a reason to revisit a specific column, not to historise a category.
