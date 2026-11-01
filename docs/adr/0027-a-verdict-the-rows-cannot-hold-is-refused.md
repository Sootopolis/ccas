# A verdict a player's rows cannot hold is refused before the save, and reported

**Status:** Accepted, 2026-11-01 (#403). Builds on [0026](0026-a-join-the-rows-cannot-place-changes-no-row.md), which fixed the joins known to break a save; this limits the damage from the next one nobody knows about.

## Context

A reconcile saves a whole club in one transaction, so one player's rows breaking a `club_member` constraint fails every reconcile of the club, History's and Recruitment's included, until the roster changes or someone corrects the rows by hand. That has happened twice (#314, #401). Each time the fix covered the shape found, and classification went on assuming its output was valid until the save said otherwise.

## Decision

**Each changed player's verdict is checked before the save.** Their stored rows, with the changes applied, are tested against `club_member_window`, `club_member_no_overlap` and `club_member_pkey`. The key is included because two empty rows with one start pass the other two and still collide. The check and the save read one list of changes, so they cannot disagree about what is written.

**A verdict that fails is refused, not saved.** That player's rows stay as recorded and the rest of the club saves. Their renames, status changes and account closures still save, since none depends on their rows. The events that would report those rows changing are dropped: a join, a leave, a rejoin, a reopening or an unresolvable name. They stay resolved, so nothing later reads them as gone.

**A roster entry is read once per player.** A refused player's other writes still save, and two writes of one player with different names would leave them without a current one, since the row keeps the first and the name history the last. So of several entries reaching one player, the one under the name their profile gives is read, or else the one under the name we hold, which keeps the choice from flipping once a rename is saved.

**It is reported whichever job reconciles,** as 0026 does for a join: Membership's report counts it and lists it under `[MEMBERSHIP NOT RECORDED]`, and any other reconcile logs a warning.

**`StoredRoster.close` no longer corrects an end before the start.** It used to move the end to the start, leaving an empty row while the report gave the earlier date. Now the row fails the check and the player is reported, and the first roster after the join closes it.

## Alternatives rejected

- **A `SAVEPOINT` per player, letting Postgres decide.** It restates no rules and covers every constraint, but it adds round trips while the club's version lock is held, and it cannot be tested without a database. The rows are already in memory. A test compares the Scala rules with Postgres, so the restatement cannot drift unnoticed.
- **Keep moving the end, and report the move.** That still records an end nothing observed, at a date Chess.com may have got wrong.

## Consequences

- The check covers `club_member` only. Player and snapshot writes are `ON CONFLICT DO NOTHING`, so a repeat of those cannot fail the save.
- A refusal repeats on every reconcile until the roster changes or the operator corrects the rows.
- A run with a refusal still completes, so the stale check counts it. A date-range report whose end that run covers reconciles nothing, so it shows neither the refusal nor a warning, as 0026 records for a join.
- A row left open by a refused close later gets a `last_seen` from a roster that did not list the player, because the previous roster is taken to have listed every open row ([0021](0021-membership-ends-are-windows.md)).
- With an empty row after a hidden one, a list's join at or after the empty row used to count as overlapping a membership, and was reported only in the run that fetched the list. It now reaches this check and is refused on every run. Neither changes a row.
- A reopened row is named under `[REOPENED]` in a reconcile's report only. The date-range report reads the rows, and a reopened row keeps no trace of having been reopened.
- A rejoin known only from matches stays `[JOINED]` in a reconcile's report, because a reconcile reads no boards.
- Nothing stored changes, so rolling back means reverting the code.
