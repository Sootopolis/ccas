# Chess.com's public API: how it behaves

Chess.com documents its [published-data API](https://www.chess.com/news/view/published-data-api) as a list of endpoints and fields, not as a contract, so what it actually does has been learned by running against it. This page records what is easy to get wrong. Each entry gives the behaviour, what code must not assume because of it, and where and when it was learned, so a reader can judge whether it still holds.

Unlike an ADR, this page describes a system we do not control: edit an entry in place when Chess.com changes or an observation proves wrong. A decision taken because of an entry belongs in an ADR that links here, and code that relies on an entry cites this page and the entry's heading.

## Accounts

### Closing an account does not end its club memberships

A closed account's `status` becomes `closed`, `closed:fair_play_violations` or `closed:abuse`, and it drops out of every club's member list, which shows only active accounts. It is still a member: its own club list keeps showing the clubs it belonged to at closure, and if the account is reopened it reappears in their member lists with its original `joined`. Its profile is still served under its username, with the new `status` and its `last_online`, which precedes the closure, until the account is [renamed](#closed-and-dormant-accounts-are-renamed).

Never read a closed account's absence from a roster as leaving.

*Operator's account, 2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)); closed profiles served by name since Membership first recorded closures, 2026-03-11. On 2026-10-14, 211 of 215 closed accounts Membership had ended a row for still listed the club with that row's `joined`, 3 no longer listed it, and 1 no longer answered ([#311](https://github.com/Sootopolis/ccas/issues/311)).*

### Closed and dormant accounts are renamed

Some closed accounts are later renamed to `x-` and ten digits. In the two cases watched closely, that happened about ten days after the last visit, which matches the waiting period of Chess.com's [account deletion](https://support.chess.com/en/articles/9829268-how-can-i-delete-my-account). Fair-play closures, and accounts unused for years, have been given an `-inactive` suffix, and the freed name can go to another account. The old name then 404s or answers for someone else.

Never infer an account's status or history from its name: these patterns are observed, not documented, and an ordinary name can follow them. Fetch what a closed account still shows soon after it closes, under a name its profile has just answered to.

*Five renames recorded in `player_name` since 2026-03; one freed name re-registered within hours, 2026-09-07 ([#311](https://github.com/Sootopolis/ccas/issues/311)).*

### Usernames change, and a freed one can be taken

After a rename the old username 404s, and may later answer for whoever registers it. The API has no lookup by player id, so a renamed account is found again only through a page that shows its new name, such as a match board or a tournament ([0010](adr/0010-rename-recovery-for-usernames-and-club-slugs.md), superseded in part). Identify a player by `player_id`, never by username ([0016](adr/0016-identity-is-the-id-names-are-observations.md)).

*2026-05-07 ([#23](https://github.com/Sootopolis/ccas/issues/23)).*

## Clubs

### A member list shows each active member's current membership

`/pub/club/{slug}/members` lists active accounts only (see [closed accounts](#closing-an-account-does-not-end-its-club-memberships)), each with the `joined` of its current membership. A member who leaves and comes back gets a new `joined`; a reopened account keeps its original one. The same `joined` seen again is therefore the same membership.

Never let a save depend on this ([0026](adr/0026-a-join-the-rows-cannot-place-changes-no-row.md)).

*Operator's account, 2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)).*

### A member list is built for each request

`/pub/club/{slug}/members` comes back with `cache-control: max-age=5`, a `last-modified` equal to the response time, and a Cloudflare cache miss: the roster is current when it arrives. A reconcile can therefore date what it saw by when the response arrived.

*Measured 2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)).*

### Slugs change, and a response can answer under another one

A renamed club's old slug 404s, and may later answer for another club ([0016](adr/0016-identity-is-the-id-names-are-observations.md)). Chess.com can also answer a request under a slug it has normalised away, so take a club's current slug from the `@id` in the response, never from the slug requested.

*Renames 2026-05-07 ([#23](https://github.com/Sootopolis/ccas/issues/23)); normalisation 2026-09-16 ([#254](https://github.com/Sootopolis/ccas/issues/254)).*

### A player's club list names clubs by slug only

`/pub/player/{username}/clubs` gives each club's `name`, `url`, `joined` and `last_activity`, but no id, and no id of the player either. Match it against every slug a club has held, and resolve an unfamiliar slug before concluding that a club is not on the list ([0016](adr/0016-identity-is-the-id-names-are-observations.md)). Ask for it under a name the player's profile has just answered to, since a name can [pass to another account](#closed-and-dormant-accounts-are-renamed).

*2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)).*

### `last_activity` is not a club's last activity

Active clubs have reported timestamps more than twelve years old. Use the latest match stored for the club instead.

*2026-04-11.*

### A club can be reported missing for months

`/pub/club/{slug}` and `/pub/club/{slug}/matches` can both answer `Club "…" not found.` for months with rename recovery finding nothing, while the admins we hold for the club are still recorded as active. A renamed club answers the same way, so this does not prove the club closed. Count it apart from failures, and keep asking: [a 404 is not permanent](#a-404-is-not-permanent).

*2026-11-02 ([#236](https://github.com/Sootopolis/ccas/issues/236)): four clubs, last fully refreshed between 2026-05-30 and 2026-07-31 or never, on nearly every sweep since at least 2026-10-12.*

### A club's match list can fail while its profile answers

`/pub/club/{slug}/matches` can return 500 for weeks while the club's profile works, so one failing endpoint says nothing about the other. Do not expect a JSON body: on 2026-11-02 the 500 carried an HTML page from Cloudflare headed `421 Misdirected Request`.

*`botvinnik-chess-school`, on nearly every sweep since at least 2026-10-12 ([#236](https://github.com/Sootopolis/ccas/issues/236)).*

## Club matches

### Registration follows membership

A player can register for a club match only as a member of exactly one of its two clubs. Leaving that club before the match starts removes them from it; once it has started they stay on their side whatever they do, including joining the other club. So a player on a club's side of a started match was its member when the match started, and a player on the other side was not.

*Operator's account, 2026-10-09 ([#302](https://github.com/Sootopolis/ccas/issues/302)).*

### The match endpoint lags the game archives

For a match's games and results, prefer the players' game archives.

*Recorded by 2026-08-12 ([#208](https://github.com/Sootopolis/ccas/pull/208)).*

### A cancelled match reports itself finished

A cancelled match that still answers has `status: finished`, so `finished` does not mean the match was played. What marks one is that nobody did: no player on either side has a `board` or a game result, and both teams score 0. Either of those alone is too little to act on, since reading a played match as cancelled deletes its boards. Many cancelled matches 404 instead, and a high count of those is expected noise, not an outage.

*Shape 2026-03-24; 404s recorded by 2026-08-12 ([#208](https://github.com/Sootopolis/ccas/pull/208)). Marker measured 2026-11-03 ([#416](https://github.com/Sootopolis/ccas/issues/416)): all 172 finished daily matches stored without boards answered this way, one with no players at all, and none of 100 random matches stored with boards did.*

### A club's match list holds only its latest 500 finished daily matches

`/pub/club/{slug}/matches` lists every match in progress or registering, but under `finished` only the 500 most recent daily matches, plus its live ones. For a club playing a few dozen matches a year that reaches back to its founding; for one playing hundreds it reaches back a year or so. Players' own match lists reach years further, so a busy club's older matches are found only through them.

*Observed 2026-10-17: devon-chess's 500 reached 2009, the-great-british-empire's only January 2025, against 11,021 of its matches found through players' lists back to its founding in 2013.*

### A player's match list has no dates

`/pub/player/{username}/matches` gives each match's `name`, `url`, `@id`, `club` and `board`, and the player's `results`, but no start or end time. It cannot say what happened in a period; the player's game archives can.

*Seen in the one response at `data/test/api/playerMatches.json`, 2026-11-03 ([#414](https://github.com/Sootopolis/ccas/issues/414)).*

## Tournaments

### `status` is unreliable

Do not branch on a tournament's `status`.

*Recorded by 2026-08-12 ([#208](https://github.com/Sootopolis/ccas/pull/208)).*

## Field values

### Enum fields are not spelt alike

`league` is capitalised (`Wood`, `Legend`) and `title` is upper case (`GM`), while `status`, `time_class`, `rules`, `result`, `turn` and `visibility` are lower case, with their own oddities: `50move`, `timevsinsufficient`, `closed:fair_play_violations`. Our decoders match exactly, so take a new value's spelling from a real response rather than from a sibling field, and expect a value we do not know to fail the whole response. Decode a field as an enum only where something reads it, so a value it gains costs nothing: `league` and `turn` stay out of our decoders, and `rules` and `visibility` are kept as plain text for later use ([#414](https://github.com/Sootopolis/ccas/issues/414)).

*Observed 2026-11-02 in 389 live responses, in `data/test/api/`, and in other projects' captured profiles up to June 2026 ([#264](https://github.com/Sootopolis/ccas/issues/264)); `50move` and the variant results carry over from the decoders before that. Not in those samples, and assumed to match their siblings: the titles `WIM`, `WCM`, `WNM` and `M`, and `aborted`, `bullet` and `mod`.*

### `timeout_percent` can read 0 after a recent timeout

A player's daily `timeout_percent` can read 0 while their game archives hold a team-match timeout from the last 90 days. Do not take 0 to mean no recent timeouts: count them in the archives. Whether this is lag, rounding or something else has not been checked.

*Observed 2026-11-03 in production's `player_recruitment_cache`: 56 of 13,521 rows, against 216 rows with a non-zero team-match rate. One player was invited about 9 hours after the timeout, on 2026-10-27 ([#417](https://github.com/Sootopolis/ccas/issues/417)).*

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
