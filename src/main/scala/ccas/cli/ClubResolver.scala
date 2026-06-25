package ccas.cli

import zio.*

import ccas.api.misc.subtypes.ClubSlug

/** Resolves a command's club target from the parsed request and the config's `current_club`. Kept pure and free of the
  * HTTP client (the `--all` expansion is injected as a `fetchManaged` thunk) so it is unit-testable without a server.
  *
  * Precedence — single-club: explicit `--club` > `current_club` > usage error. Multi-club: `--all` (every managed club)
  * > explicit `--club a,b` > `current_club` > usage error. `--all` together with explicit `--club` is rejected (the two
  * express conflicting intent, so combining them is treated as a mistake rather than silently letting one win). A
  * failure carries exit code 2 (a usage error).
  */
object ClubResolver {

  val NoClubError =
    "no club specified: pass --club <slug> or set a current club with `ccas use-club <slug>`"
  val NoManagedError = "no managed clubs; add one with `ccas club add`"
  val BothError      = "--all and --club are mutually exclusive; pass one or the other"

  def single(explicit: Option[String], currentClub: Option[String]): IO[CliError, ClubSlug] =
    // Clean each source independently before `orElse`, so a blank explicit `--club` falls back to current_club rather
    // than blanking it out — mirrors how the comma-split multi path drops blank entries.
    ZIO.fromOption(blankToNone(explicit).orElse(blankToNone(currentClub)))
      .mapBoth(_ => CliError(NoClubError, 2), ClubSlug(_))

  private def blankToNone(o: Option[String]): Option[String] = o.map(_.trim).filter(_.nonEmpty)

  def multi(
    fetchManaged: => Task[List[String]],
    explicit: List[String],
    all: Boolean,
    currentClub: Option[String]
  ): Task[NonEmptyChunk[ClubSlug]] =
    if (all && explicit.nonEmpty) { ZIO.fail(CliError(BothError, 2)) }
    else if (all) { fetchManaged.flatMap(toClubs(_, CliError(NoManagedError, 2))) }
    else { toClubs(if (explicit.nonEmpty) explicit else currentClub.toList, CliError(NoClubError, 2)) }

  // Trim and drop blanks so a padded/empty slug (from --club, current_club, or a hand-edited config) can't reach the
  // API path verbatim — `ClubSlug.normalize` only lowercases.
  private def toClubs(slugs: List[String], ifEmpty: => CliError): IO[CliError, NonEmptyChunk[ClubSlug]] =
    ZIO.fromOption(NonEmptyChunk.fromIterableOption(slugs.map(_.trim).filter(_.nonEmpty).map(ClubSlug(_)))).orElseFail(ifEmpty)
}
