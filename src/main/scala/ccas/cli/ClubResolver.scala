package ccas.cli

import zio.*

import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.cli.config.CurrentClubRef

/** A resolved club target: the slug to send plus, when known, the stable [[ClubId]] to resolve by server-side. Only a
  * `current_club` pointer carries an id (parsed from its `<id>:<slug>` form); a freshly-typed `--club <slug>` and the
  * `--all` managed expansion resolve by slug alone, so their id is `None`. Sending the id makes a renamed current club
  * still resolve instead of 404-ing on its stale slug (#176).
  */
final case class ClubTarget(clubId: Option[ClubId], slug: ClubSlug)

/** Resolves a command's club target(s) from the parsed request and the config's `current_club`. Kept pure and free of
  * the HTTP client (the `--all` expansion is injected as a `fetchManaged` thunk) so it is unit-testable without a server.
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

  def single(explicit: Option[String], currentClub: Option[String]): IO[CliError, ClubTarget] =
    // Clean each source independently before falling back, so a blank explicit `--club` falls back to current_club
    // rather than blanking it out — mirrors how the comma-split multi path drops blank entries.
    blankToNone(explicit) match {
      // An explicit `--club <slug>` is a freshly-typed slug the CLI has no id for.
      case Some(slug) => ZIO.succeed(ClubTarget(None, ClubSlug(slug)))
      case None =>
        blankToNone(currentClub) match {
          case Some(raw) =>
            val ref = CurrentClubRef.parse(raw)
            ZIO.succeed(ClubTarget(ref.clubId, ClubSlug(ref.slug.trim)))
          case None => ZIO.fail(CliError(NoClubError, 2))
        }
    }

  private def blankToNone(o: Option[String]): Option[String] = o.map(_.trim).filter(_.nonEmpty)

  def multi(
    fetchManaged: => Task[List[String]],
    explicit: List[String],
    all: Boolean,
    currentClub: Option[String]
  ): Task[NonEmptyChunk[ClubTarget]] =
    if (all && explicit.nonEmpty) { ZIO.fail(CliError(BothError, 2)) }
    else if (all) { fetchManaged.flatMap(slugs => toTargets(slugs.map(idless), CliError(NoManagedError, 2))) }
    else if (explicit.nonEmpty) { toTargets(explicit.map(idless), CliError(NoClubError, 2)) }
    else { toTargets(currentClub.toList.map(fromCurrent), CliError(NoClubError, 2)) }

  private def idless(slug: String): (Option[ClubId], String) = (None, slug)

  private def fromCurrent(raw: String): (Option[ClubId], String) = {
    val ref = CurrentClubRef.parse(raw)
    (ref.clubId, ref.slug)
  }

  // Trim and drop blanks so a padded/empty slug (from --club, current_club, or a hand-edited config) can't reach the
  // API path verbatim — `ClubSlug.normalize` only lowercases.
  private def toTargets(
    pairs: List[(Option[ClubId], String)],
    ifEmpty: => CliError
  ): IO[CliError, NonEmptyChunk[ClubTarget]] =
    ZIO
      .fromOption(
        NonEmptyChunk.fromIterableOption(
          pairs.map((id, s) => (id, s.trim)).filter((_, s) => s.nonEmpty).map((id, s) => ClubTarget(id, ClubSlug(s)))
        )
      )
      .orElseFail(ifEmpty)
}
