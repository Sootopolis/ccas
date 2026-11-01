# A join a member's rows cannot place changes no row, and is reported

**Status:** Accepted, 2026-11-01 (#401). Narrows [0022](0022-a-closure-hides-a-membership.md)'s rule that the same `joined` seen again reopens a row: it now holds for a player's latest row only. Extended by [0027](0027-a-verdict-the-rows-cannot-hold-is-refused.md) (#403): a verdict the rows cannot hold, whatever produced it, is refused before the save.

## Context

A club's member list gives each active member the `joined` of their current membership: the start of what we hold as their latest row, or, for someone who left and came back, a later date ([chess-com-api.md](../chess-com-api.md), "A member list shows each active member's current membership"). Classifying a roster relied on that without checking it.

A join before the latest row's start had two outcomes. Most failed the save on `club_member_pkey` or `club_member_no_overlap`, and went on failing every reconcile of the club, History's and Recruitment's included, until the roster changed. One clear of every other row saved, leaving the latest row empty beside a new one.

A later join could fail the same way with Chess.com behaving as documented. An ended row's `until` comes from our clock, taken once the roster has arrived, and `joined` is in whole seconds. A member who rejoins as a roster built just before that is arriving has their row end after their new `joined`, so the next roster's join falls inside the row.

Neither is known to have happened. #324's property found the gap, and on 2026-11-01 Neon held no row with `since = until`, the mark the silent outcome leaves. One way an older join could arise is a closed account's club list giving a different `joined` from the roster's for the same membership, so that the row opened from the list starts later than the date the roster lists once the account reopens.

## Decision

**The player's latest row decides.** A join at its start is the same membership, and the row reopens. A later join is a new membership: a new row opens, and the latest row ends at the join if it was still running then. That includes a row whose `until` falls after the join, if it was last seen before the join, since that `until` is only our own reading of when they had gone.

**A join the rows contradict cannot be placed.** That is a join before the latest row's start, or one inside an ended row at or before when it was last seen: a roster normally listed the old membership then, so this is not a fresh join. A hidden row is not held to the second test, because its last sighting can be only the account's last visit, which does not show membership. A join that cannot be placed changes no row, and the player still counts as listed, so nothing later reads them as gone. This is how 0022 already treats a closed account's club list that would overlap a recorded membership. A rename or a change of status still saves, since neither depends on the join.

**It is reported whichever job reconciles.** Membership's report counts the join and lists it under `[ROSTER JOIN UNSETTLED]`. Any other reconcile logs a warning instead, because History's, Recruitment's and those `MembershipApp`'s `--since` mode runs first print no changes.

## Alternatives rejected

- **Fail the reconcile with a clear error.** The message improves, but one player still stops three jobs for the whole club until the roster changes or someone corrects the rows by hand.
- **Leave it, on the strength of the documented behaviour.** That page records what has been observed, not a contract, and a join clear of the other rows would damage the latest one with no sign of it.
- **Report every join inside an ended row.** A member who had simply rejoined would stay out of the counts until someone corrected the rows by hand, because of a race our own clock creates.
- **Treat every join inside an ended row as a rejoin.** A roster listing 2022 against a row last seen in 2024 would cut the row short and overwrite when it was last seen, with only an ordinary rejoin to show for it.
- **Allow a tolerance around `until`.** A time constant would have to guess how far apart the two clocks can drift, and the last sighting needs none.

## Consequences

- Nothing corrects the rows when a join cannot be placed. The report or warning repeats until the roster changes or the operator corrects them by hand.
- A join inside an ended row, after it was last seen, replaces the `until` we recorded, as a rejoin sets one on an open row.
- An open row records no last sighting, so it is not held to that test. A join after its start ends it at that join, even one before the club's previous roster, as a rejoin always has.
- Member counts come from the rows, so a reported player whose latest row has ended is not counted, though the roster lists them, and the report's line says so. One whose latest row is hidden stays hidden, so a reconcile's total counts them and a date-range report's does not.
- The last sighting is our clock's too, so a rejoin within clock drift of it would be reported rather than recorded. Reporting is the safe side of that.
- `MembershipApp`'s `--since` mode, which `ccas membership` does not offer, reads only the database, so its report never lists such a join and can say "No changes". The warning from any reconcile it runs first is all that run shows.
- Nothing stored changes, so rolling back means reverting the code.
