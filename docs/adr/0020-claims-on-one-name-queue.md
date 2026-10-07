# Claims on one name queue; they are not refused

**Status:** Accepted, 2026-10-01 (#298). Refines [0016](0016-identity-is-the-id-names-are-observations.md): it changes how its name tables take concurrent writes, not what they hold. Amended by #300: no transaction fetches from Chess.com any more, and the client refuses a fetch inside `withTransaction`. The `HistoryProcessing` fetch that two of the rejected alternatives below cite is gone; each still stands on its other ground.

## Context

Recording that a player or club holds a name is a read-then-write: close whoever holds it now, then open the new holder. `player_name_current` and `club_name_current` allow one current holder per name. The `player` / `club` row that the surrounding write locks already serialises two writers for one *holder*. Nothing serialised two holders claiming one *name*. When two such writes overlap, neither sees the other's uncommitted row, both open the name, and the unique index refuses whichever commits second.

#276 chose that refusal on purpose: one of two holders observed at once must be stale, and the loser's next refresh re-reads it. #298 showed that the reasoning does not hold:

- The index refuses by commit order, not by staleness, so the observation it keeps may be the stale one.
- When the same two writes do not overlap, both succeed and the later one takes the name. The outcome depended on timing alone, which is what made the recruitment test flake.
- The refused transaction carries more than the name: a recruitment verdict, a membership roster batch, a club refresh. Recruitment records the loser as `Error`, so a candidate who passed every filter goes uninvited that run, and there is no next refresh within the run to re-read it.

## Decision

**A write that moves a name first takes a transaction-scoped advisory lock on every name it touches: the name it claims and the name the holder gives up.** Only then does it read the clock it stamps the change with. A second writer claiming the same name waits for the first to commit, then sees the first writer's row and closes it, as if the two had never overlapped. Reading the clock after the lock keeps the windows in commit order.

The name the holder gives up needs the lock as much as the name it claims. Suppose a holder moves off a name while another writer claims it. Without the lock, the holder can open its new window before the claim has closed its old one, and the no-overlap constraint refuses that.

When the name already stands (the common path), nothing is locked and no round trip is added.

**The locks are bounded.** Each writer holds its key space (players or clubs) in shared mode, and then each of its names exclusively, keyed by the name's hash and taken in key order. A call that would take more name locks than `NameLock.MaxNameKeys` instead holds the whole space exclusively, which excludes every per-name writer at once. Without the bound, a large club's first membership run would take one lock per member. Advisory locks share one fixed-size table with every other lock in the database, and once that table is full, this transaction and any other that needs a lock fail with `out of shared memory`.

**A transaction records names once.** Each call takes its locks in key order, but a second call's keys can sort below the first's, and then two such transactions can wait on each other in a cycle. That is why `Player.writeBatch` records a membership batch's inserts and updates in one call.

**Every advisory lock goes through `AdvisoryLock`,** which registers the number that names each kind of lock. Postgres gives those numbers no meaning, so two features that picked the same one would wait on, deadlock with, or misread each other's locks without anything noticing. A test therefore fails on a number used twice, and on an advisory-lock call anywhere else in the application or its scripts. That catches a maintainer who never heard of the registry, because taking a lock means writing the function's name. A number names its kind of lock to every build running against the database, so numbers are added but never changed or reused. They carry a `0x0CCA` prefix, so code that bypasses both checks, such as SQL assembled at runtime or a hand-run session, is unlikely to pick the same one.

Alternatives rejected:

- **Keep the refusal and retry.** A retry in each caller fixes only the callers that retry. A single retry in `withTransaction`, on the unique violation or on the serialisation failure `SERIALIZABLE` would raise instead, covers every caller, but only if every transaction is safe to run twice. `HistoryProcessing` fetches from Chess.com inside its transaction, so a retry would repeat the fetch, and every future transaction would have to stay safe to repeat too.
- **Lock the conflicting row.** The row is the other writer's uncommitted insert, which this transaction cannot see.
- **A fixed set of hashed keys.** It bounds the locks without switching modes, but a writer then waits for any holder whose names share a key, not just for one moving the same names. `HistoryProcessing` holds its transaction open across a Chess.com fetch, so an unrelated write could wait on that fetch.

## Consequences

- The concurrency tests force the overlap rather than hoping for it, and assert which lock the second writer queued on and that it stamped later than the first was released.
- Between two simultaneous observations, the last writer wins, which is arbitrary, just as it already was without overlap. The next observation of either holder corrects it.
- **A long transaction holds its names for its whole length.** A writer moving the same name waits for it, as does a batch past the bound, which needs the whole space. Before this change, a writer claiming the same name waited just as long on the unique index, and then failed. A Chess.com fetch inside the transaction would stretch that wait to a network round trip, which is why the client refuses one (#300).
- `backfill` writes the name tables directly and takes no lock. It runs at boot and uses `ON CONFLICT DO NOTHING`, so it does not need one.
