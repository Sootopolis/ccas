# Chess.com's public API: how it behaves

Chess.com documents its [published-data API](https://www.chess.com/news/view/published-data-api) as a list of endpoints and fields, not as a contract, so what it actually does has been learned by running against it. This page records what is easy to get wrong. Each entry gives the behaviour, what code must not assume because of it, and where and when it was learned, so a reader can judge whether it still holds.

Unlike an ADR, this page describes a system we do not control: edit an entry in place when Chess.com changes or an observation proves wrong. A decision taken because of an entry belongs in an ADR that links here, and code that relies on an entry cites this page and the entry's heading.

## Accounts

### Closing an account does not end its club memberships

A closed account's `status` becomes `closed`, `closed:fair_play_violations` or `closed:abuse`, and it drops out of every club's member list, which shows only active accounts. It is still a member: its own club list keeps showing the clubs it belonged to at closure, and if the account is reopened it reappears in their member lists with its original `joined`. Its profile is still served under its username, with the new `status` and its `last_online`, which precedes the closure. Chess.com is not known to rename an account when closing it.

Never read a closed account's absence from a roster as leaving.

*Operator's account, 2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)); closed profiles served by name since Membership first recorded closures, 2026-03-11.*

### Usernames change, and a freed one can be taken

After a rename the old username 404s, and may later answer for whoever registers it. The API has no lookup by player id, so a renamed account is found again only through a page that shows its new name, such as a match board or a tournament ([0010](adr/0010-rename-recovery-for-usernames-and-club-slugs.md), superseded in part). Identify a player by `player_id`, never by username ([0016](adr/0016-identity-is-the-id-names-are-observations.md)).

*2026-05-07 ([#23](https://github.com/Sootopolis/ccas/issues/23)).*

## Clubs

### A member list shows each active member's current membership

`/pub/club/{slug}/members` lists active accounts only (see [closed accounts](#closing-an-account-does-not-end-its-club-memberships)), each with the `joined` of its current membership. A member who leaves and comes back gets a new `joined`; a reopened account keeps its original one. The same `joined` seen again is therefore the same membership.

*Operator's account, 2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)).*

### Slugs change, and a response can answer under another one

A renamed club's old slug 404s, and may later answer for another club ([0016](adr/0016-identity-is-the-id-names-are-observations.md)). Chess.com can also answer a request under a slug it has normalised away, so take a club's current slug from the `@id` in the response, never from the slug requested.

*Renames 2026-05-07 ([#23](https://github.com/Sootopolis/ccas/issues/23)); normalisation 2026-09-16 ([#254](https://github.com/Sootopolis/ccas/issues/254)).*

### A player's club list names clubs by slug only

`/pub/player/{username}/clubs` gives each club's `name`, `url`, `joined` and `last_activity`, but no id. Match it against every slug a club has held, and resolve an unfamiliar slug before concluding that a club is not on the list ([0016](adr/0016-identity-is-the-id-names-are-observations.md)).

*2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)).*

### `last_activity` is not a club's last activity

Active clubs have reported timestamps more than twelve years old. Use the latest match stored for the club instead.

*2026-04-11.*

## Club matches

### Registration follows membership

A player can register for a club match only as a member of exactly one of its two clubs. Leaving that club before the match starts removes them from it; once it has started they stay on their side whatever they do, including joining the other club. So a player on a club's side of a started match was its member when the match started, and a player on the other side was not.

*Operator's account, 2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)).*

### The match endpoint lags the game archives

For a match's games and results, prefer the players' game archives.

*Recorded by 2026-08-12 ([#208](https://github.com/Sootopolis/ccas/pull/208)).*

### A cancelled match reports itself finished

A cancelled match that still answers has `status: finished`, so `finished` does not mean the match was played. Many cancelled matches 404 instead, and a high count of those is expected noise, not an outage.

*Shape 2026-03-24; 404s recorded by 2026-08-12 ([#208](https://github.com/Sootopolis/ccas/pull/208)).*

## Tournaments

### `status` is unreliable

Do not branch on a tournament's `status`.

*Recorded by 2026-08-12 ([#208](https://github.com/Sootopolis/ccas/pull/208)).*

## Errors and limits

### A 404 can mean absent or broken

A body of the form `"X \"id\" not found."` reports that the thing does not exist, and is an answer. Any other 404 body is a malfunction: on 2026-09-08, 71 club slugs returned an internal-error 404 (`code: 3024`) every day ([0019](adr/0019-a-reported-404-is-an-answer.md)).

*2026-09-09 ([#234](https://github.com/Sootopolis/ccas/issues/234)).*

### A 404 is not permanent

"Not found" bodies are timeline-unstable, and a username or slug can go from 404 to 200 when someone registers it later ([0010](adr/0010-rename-recovery-for-usernames-and-club-slugs.md), superseded in part).

*2026-05-07 ([#23](https://github.com/Sootopolis/ccas/issues/23)).*

### Rate limits are unpublished

Parallel bursts draw 429s. Cloudflare sometimes answers with a challenge 403, which says nothing about the resource; other 403s are permanent ([0012](adr/0012-gate-based-adaptive-throttle.md)).

*2026-04-01.*

### Conditional requests are ETag-only

The origin ignores `If-Modified-Since` in any format, and sends `Last-Modified` in a form RFC 7231 does not allow ([0007](adr/0007-response-caching-in-postgres.md)).

*2026-04-17.*
