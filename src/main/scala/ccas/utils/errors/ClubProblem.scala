package ccas.utils.errors

import ccas.utils.json.EnumJson

/** Typed discriminant for a club-scoped request that couldn't run, carried on the wire alongside the human-readable
  * `error` string so the CLI branches on a value instead of matching `error.startsWith("Club not found")`.
  *
  *   - [[NotFound]]: the slug/id isn't in our DB. (Once an opt-in Chess.com verify exists, #180, this will also mean
  *     "not on Chess.com either"; today the server only consults the local DB, so it means "not local".)
  *   - [[Problematic]]: the club is known but unusable — currently a tombstoned (`_stale_<id>`) slug that lost its
  *     canonical name and hasn't been recovered.
  *
  * Minimal by design: `Renamed`/`NotManaged` arms arrive with the resolver ADT's later slices (rename recovery at
  * submission, and the managed-status gate #177). Wire form is snake_case (`not_found` / `problematic`).
  */
enum ClubProblem {
  case NotFound, Problematic
}
object ClubProblem extends EnumJson[ClubProblem]
