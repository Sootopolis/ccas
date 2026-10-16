# A membership's ends are windows, taken from when the roster was seen

**Status:** Accepted, 2026-10-10 (#302). Builds on [0017](0017-what-earns-a-history-table.md), which gave `club_member` its two ends and asked for overlap between them to be excluded. Amended by [0022](0022-a-closure-hides-a-membership.md) (#311): a row a closed account hides has a `last_seen` and no `until`, so `club_member_window` asks only that `until` come with `last_seen`, and a closed account's row no longer ends at its last visit.

## Context

A member row's `until` recorded when a reconcile noticed the player gone, and nothing recorded when they were last known to be in. Membership sees a roster only when it runs, so a leave happens somewhere between two runs, and the row kept only the later one. A count of members on a past date, or a report over a range, could not tell "had left" from "not yet noticed".

The missing half was knowable: the previous run had listed the player. But runs live in `membership_run`, an operational log that did not even record the reconciles History and Recruitment start with, and membership facts read back out of a log change whenever the log does.

## Decision

**Each end of a membership is a window.** `last_seen` is the last time the player was known to be a member, `until` the first time they were known not to be; both are empty while the membership is current. `club_member_window` keeps them together and in order (`since ≤ last_seen ≤ until`), and `club_member_no_overlap` excludes overlap between one player's rows in one club, using `btree_gist` as the name tables do ([0016](0016-identity-is-the-id-names-are-observations.md)).

**When a roster was seen has a home of its own.** `club_roster_coverage` holds, per club, the first and last time Membership fetched the full roster. It is bookkeeping in 0017's sense, what we did rather than what the club did, but the windows are defined by it, so Membership writes it in the transaction that writes the rows the roster implied, and never moves either bound inward. A roster is built for each request, so the moment it arrived is the moment it describes ([chess-com-api.md](../chess-com-api.md), "A member list is built for each request").

**A row's ends are copied onto it when it closes.** `until` is the roster that no longer listed the player, or, for a rejoin, the new join date. `last_seen` is the club's previous roster, which listed every open roster row, held within the row; a guessed row was never on a roster, so it was known in only at its start. Nothing reads the runs afterwards to reinterpret a row, so `membership_run` stays an operational log: pruning it would change no membership.

**Every reconcile is a run.** History's and Recruitment's are recorded with trigger `Dependency`, so the stale check counts them. Membership's report still measures from its own previous run, so a join a dependency saw is reported as before.

**A rejoin closes the old row at the new join date, before the new row opens.** The player had left by then, and the new row would otherwise overlap the old one while it was still open.

**An observed row replaces a guessed one.** History still guesses memberships from matches. A roster row that overlaps one of those guesses deletes it rather than failing on the constraint.

## Alternatives rejected

- **Write `last_seen` on every current row at every run.** One write per member per run, for a fact the coverage table records once per club.
- **Work `last_seen` out from `membership_run` when it is read.** Membership facts would depend on an operational log, and pruning or re-triggering runs would rewrite them.
- **Keep every roster.** Every member at every run; the rows and the coverage table are its run-length encoding, provided each roster is applied whole and in one transaction.

## Consequences

- A count of members on a past date still uses `until`: a player counts until known to be out, which is also when the report dates their leave, so counts and events agree.
- Coverage for existing clubs is filled from their runs' completion times, a few seconds after each roster arrived.
- Older binaries write ended rows without `last_seen` and cannot decode `Dependency`: rolling back means dropping `club_member_window` and re-labelling those runs.
- A closed account's row still ends at its last visit, though closing an account does not end its memberships ([chess-com-api.md](../chess-com-api.md), "Closing an account does not end its club memberships"). #302 corrects that separately.
- History's guess for an active player who no longer lists the club still ends at the match it saw them in, which they were still in, so both its ends are that match. A report gives an end like that only as when the player was last seen; #302 replaces the guesses with memberships computed from matches.
