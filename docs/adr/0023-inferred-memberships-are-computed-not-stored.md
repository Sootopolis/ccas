# A membership only matches show is inferred when asked for, not stored

**Status:** Accepted, 2026-10-17 (#302). Amends [0021](0021-membership-ends-are-windows.md) and [0022](0022-a-closure-hides-a-membership.md): History no longer guesses memberships or reads club lists, so no observed row replaces a guess.

## Context

A player on a club's side of a started match was its member when the match started, and one on the other side was not ([chess-com-api.md](../chess-com-api.md), "Registration follows membership"). History turned the first board it met a player on into a guessed `club_member` row, flagged `since_approximate`, if the player had no row: the match's start was the join, and for an active player who no longer listed the club the leave as well. It never revisited a guess. On 2026-10-17 Neon held 1,898 guessed rows, 1,514 of them beginning and ending at the one match they came from, and 1,956 club and player pairs with matches for the club and no observed row at all. Membership carried a special case for each kind of guess it met.

What matches add is almost all about the time before a club's first full roster. On 2026-10-17 no match since a club's first roster fell outside one of the player's observed rows.

## Decision

**`club_member` holds what a roster or a club list showed, and nothing else.** History writes no member rows and fetches no club lists, and the guessed rows and `since_approximate` are gone.

**A membership only matches show is inferred when a report asks for it.** A match for the club outside every observed row shows the player a member at its start. Consecutive such matches are one membership unless something between them shows the player out: a match against the club; the start of an observed row, which begins a new membership, since a member list gives the current membership's `joined`; or the club's first full roster, which did not list them. From that roster on, each such match stands alone: the rosters between two of them are not kept, so nothing says the player stayed a member across them. An inferred membership carries its first and last match for the club and how many there were. When it began and ended is unknown, and it claims neither.

**Asking whether a player was ever a member reads the boards as well as the rows.** Recruitment's `excludeFormerMembers` rules out a player with an ended row or a board on the club's side, and History starts from every player with a row or such a board.

**History crawls the match list of each player it meets on the club's side and did not start from,** once, whether it knew the player's name already or had to look it up. A player first met as an opponent is crawled when met again on the club's side.

**The date-range report counts from the rows alone,** a hidden row until its `last_seen`, so every change in the counts has an event behind it. Inferred memberships that overlap the range get a section of their own, and a join that follows one is reported as a rejoin of it. The reconcile's summary keeps its counts. Its previous count is taken when the previous run completed, after the roster that gives a closure its `last_seen`, so counting from rows would make its delta depend on whether another job reconciled in between.

## Alternatives rejected

- **Store inferred memberships and rebuild them after each History run.** A first implementation did. Every reader of `club_member` then had to tell those rows from observed ones, and a change of rule needed a rebuild, for rows only the report reads.
- **Correct a guessed row at each sighting.** History sees one sighting at a time and never revisits a processed match, so the rows already stored would stay wrong.
- **Join every match outside a row into one membership.** Matches years apart, or with matches against the club between them, would read as one membership.
- **Give an inferred membership approximate ends.** The evidence brackets a leave only between the last match for the club and whatever next shows the player out, which can be years later, and a date would be read as a fact.

## Consequences

- Migrating deletes the guessed rows, 1,898 on 2026-10-17, none of them open and each with a board on the club's side, then drops `since_approximate`. Neither build runs on the other's schema. Rolling back means re-adding the column with `DEFAULT false`, after which the old build guesses again.
- A report over a range before a club's first roster lists the memberships its matches show, but counts only members with observed rows.
- The reconcile's summary calls a join after an inferred membership a join, as it already did, and the date-range report calls it a rejoin. On 2026-10-17, 39 observed rows followed such a membership.
- The first History run after migrating crawls the 52 players, as of 2026-10-17, who played for a club with History runs and whose match lists were never fetched.
- Live club matches store no boards, so a player seen only in them leaves no evidence.
- A closed account whose club list is not yet kept is read as out at the first roster, though it may still have been a member. The inferred membership claims no end, so the report says nothing false about it.
