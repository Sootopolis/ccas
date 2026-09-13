# A reported 404 is an answer, not a failure

**Status:** Accepted, 2026-09-09 (#234). Extends [0007](0007-response-caching-in-postgres.md), whose result type this renames and adds a variant to.

## Context

Chess.com answers "no such club" and "our backend broke" with the same status code. `HttpStatusException.classify` already separates them by body — a `"X \"id\" not found."` shape becomes `ReportedNotFound`, everything else stays `HttpStatusException` — but both left the client through the error channel, so both were counted as failures.

The visible cost was an operator-facing lie: `ClientStatsAccumulator.summary` prints `requests ($failures failed)`, and a run whose 404s were all absences reported a failure count that meant nothing. The structural cost was larger. Whether an absent resource is acceptable is a property of the caller — a rename probe expects it, a club the operator typed does not — but the client was the thing deciding, and callers recovered the distinction with `catchSome`/`onNotFound` several frames above the fetch.

An earlier attempt made the caller *declare* expectation to the client, via a scoped region backed by a `FiberRef`. It worked and was wrong: the declaration was ambient, its correctness depended on the region staying exactly one fetch wide, and the type system said nothing about either.

## Decision

**Absence is a value the client returns; malfunction stays in the error channel.**

`CacheableResult[T]` becomes `FetchResult[T]` and gains a fifth variant, `Missing(cause: ReportedNotFound)`. `getCacheable` becomes `getResult`. A reported-not-found therefore never becomes an error inside the client: the exchange completed, so it counts as a completed exchange and lands in `successes`, and the app decides what absence means.

`foldZIO` is total — `ifMissing`, `ifUnchanged`, `ifChanged` — so every existing consumer had to answer the question once, at the point where the answer is known. `foldPresentZIO` is the shorthand for callers whose answer is "fail", which raises the `ReportedNotFound` that the rename-recovery combinators key on; that is a decision spelled at the call site, not a default. `get` stays loud by failing through `Missing.getValue`, so the majority of call sites — the ones that must never silently accept absence — are unchanged, and `getOptional` serves the sites that want `None`.

Errors are the things that are not answers: transport failures, 5xx, 429, decode failures, and a 404 carrying an internal-error body. They stay where the retry schedules, the failure window and the throttle already act on them.

Alternatives rejected: `Task[Either[ReportedNotFound, T]]` (a second vocabulary for outcomes, alongside the one this type already is); a separate `FetchOutcome` wrapping `CacheableResult` (honest about the two axes, but absence wants to be cacheable — see below — and nesting puts it outside the axis it belongs to); returning `Option[T]` universally (taxes the majority of sites, and a lazy `getOrElse` would quietly discard exactly the absences an operator needs to see).

## Consequences

- **The counters change meaning at this commit,** and nothing in `client_stats` marks the boundary — the same hazard [0006](0006-pacing-ema-measures-the-http-exchange-only.md) records for `latency_*`. Rows before it count a reported 404 in `failures` and `errors_other`; rows after it count it in `successes`.
- **`api_fetch_failure` is written explicitly for a `Missing`.** The row used to come from `rawGet`'s `tapError`, which no longer fires for an absence. It is kept deliberately: on 2026-09-08 that table was the only evidence that 71 club slugs return a `code: 3024` internal-error 404 every day. The negative fact's proper home is a cache entry with a TTL — the back-off in #236 — and until that exists this table is it.
- **The classification is now load-bearing.** Before, misclassifying an absence as an error cost an inflated counter; now it decides which channel the outcome leaves by, and so which branch an app takes. The rule is still a substring match on the message, which #3 replaces with a parsed body and an explicit code table. What that issue must add is an `Unknown` state that is counted, so upstream rewording surfaces as a number moving rather than as behaviour silently changing.
- **A cache hit still hides absence, and that is the first thing negative caching must reconcile.** `getResult` serves a `Fresh` entry within `max-age` without asking Chess.com, so a resource that has since gone keeps returning its cached body until the entry ages out; `Missing` is only ever produced by a request that actually left the process. That was equally true when absence was an error, but it is worth stating now that apps branch on the outcome.
- **Absence is cacheable, and this shape leaves room for it.** `Missing` sitting beside `Fresh` means "we knew it was missing without asking" is expressible later as a variant property rather than a parallel mechanism, which is also where the previous point gets resolved.
