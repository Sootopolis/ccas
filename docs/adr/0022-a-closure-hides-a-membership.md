# A closure hides a membership, and the account's club list settles it

**Status:** Accepted, 2026-10-14 (#311). Builds on [0021](0021-membership-ends-are-windows.md), whose `club_member_window` it relaxes. Amended by [0023](0023-inferred-memberships-are-computed-not-stored.md) (#302): History neither guesses memberships nor fetches club lists, so a closed account it finds on a board is settled by Membership's kept lists alone. Amended by #401: a `joined` seen again reopens the player's latest row only ([0026](0026-a-join-the-rows-cannot-place-changes-no-row.md)).

## Context

Closing an account drops it from every club's member list but does not end its memberships: its own club list keeps the clubs it belonged to at closure, and a reopened account comes back with its original `joined` ([chess-com-api.md](../chess-com-api.md), "Closing an account does not end its club memberships"). Membership read the drop as a leave and ended the row at the account's last visit. On 2026-10-14, 211 of the 215 rows ended that way on Neon belonged to accounts whose club list still showed the club, with the row's `joined`.

Ending the row also left a wedge. A reopened account listed again with its original `joined` produced a row with the same key as the ended one, and the duplicate failed every later reconcile of the club, History's and Recruitment's included.

## Decision

**A closure hides a membership rather than ending it.** A hidden row has a `last_seen` and no `until`, so `club_member_window` now asks only that `until` come with `last_seen`, still in the order `since ≤ last_seen ≤ until`. Its `last_seen` is the later of the club's previous roster and the account's last visit: the list shows the club at closure, which came after the last visit.

**The account's club list decides.** When a member the roster drops turns out to be closed, Membership fetches the account's club list in the same reconcile, before Chess.com is likely to rename it. A list without the club means the player left before closing, and the row ends at this roster as a leave; the reconcile has just confirmed the club's slug, which is the one a list fetched now gives it. Any other answer, the club listed or no list to be had, hides the row: an absence from the roster is not a leave.

**A list is fetched under a name the profile has just answered to, and kept.** A club list carries no player id, and a closed account's name can stop answering or pass to another account, so the profile comes first and its id is checked. `player_club_list` keeps each list with the profile's last visit, or as having found nothing, and a list speaks for a club under every slug the club has held.

**Kept lists settle the rows closures ended before this.** Each Membership reconcile fetches the lists of up to 50 of the club's closed accounts, linked to it by a row or by a board on its side, that have none kept since their state last changed. It then applies every kept list: an ended row starting at the listed `joined` is hidden instead, and a listed membership no row records opens hidden, replacing any guess it overlaps. A list whose `joined` would overlap a recorded membership changes nothing, and is reported, as is a name that no longer answers.

**The same `joined` seen again reopens the row.** A roster listing a player with the `joined` of their latest row, hidden or ended, clears its `last_seen` and `until` instead of inserting a new row. A hidden row with an earlier `joined` closes at the new one, as a rejoin. Once any job has seen the account active again, its hidden row is one the roster should list, and an absence from the roster is checked like any member's.

## Alternatives rejected

- **Reopen the rows ended at closures in the migration.** It would take `until` off rows on a guess, and 3 of the 215 belong to players who had left before closing. Only a list reopens a row.
- **Hide every closure without asking for the list.** A player who left and then closed between two rosters would stay a member.
- **Fetch every list at once.** 605 closed accounts are linked to the two tracked clubs; a cap per run spreads them across runs and within the rate limits.
- **Recover the names that no longer answer.** Rename recovery costs several requests per account, for accounts that are likely deleted. They are reported instead.

## Consequences

- Member counts still count active accounts only, so a hidden member is not counted, as before. The report dates a closure by the hidden row's `last_seen` and lists it apart from leaves.
- A row whose account's name no longer answers keeps the end a closure gave it. Its list is asked for again only once the player is seen under another name, which changes their state.
- A slug the club once held and another club holds now would make that club's entry on a list speak for this one.
- History hides a closed account's membership its club list shows. Its guesses for closed accounts still end at the last visit until #302 removes them.
- Rolling back means ending or deleting the hidden rows before restoring the old constraint, which refuses them.
