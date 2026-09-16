package ccas.cli

import zio.*

import ccas.analysis.apps.ClubQuery
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.cli.config.CurrentClubRef

/** A club a command is targeting: how the server should look it up, and what to call it until it answers. A
  * `current_club` pointer and `--club-id` send the stable id, which resolves whatever the club has since been renamed
  * to (#176); a typed `--club <slug>` and the `--all` managed expansion send the name. The label is only ever for
  * display — for an id from `current_club` it is the slug last seen, which is friendlier than the number.
  */
final case class ClubTarget(query: ClubQuery, label: String) {

  /** Whether the server is being asked by id, which decides if a slug it answers with may refresh the pointer. */
  def addressedById: Boolean = query match {
    case ClubQuery.ById(_)   => true
    case ClubQuery.BySlug(_) => false
  }

  /** The slug for the commands still keyed by one — blacklist, criteria, `recruit --report` (#254 step 3 retires
    * them). Every target they can be given carries a name: a typed `--club`, or a pointer whose label is the slug last
    * seen. Only `--club-id` labels a target with its id, and none of them offers it.
    */
  def displaySlug: ClubSlug = ClubSlug(label)
}

object ClubTarget {

  /** A club named by its id, which is all the label can say until the server answers with a name. */
  def byId(clubId: ClubId): ClubTarget = {
    val query = ClubQuery.ById(clubId)
    ClubTarget(query, query.describe)
  }
}

/** Resolves a command's club target(s) from the parsed request and the config's `current_club`. Kept pure and free of
  * the HTTP client (the `--all` expansion is injected as a `fetchManaged` thunk) so it is unit-testable without a server.
  *
  * Precedence — single-club: explicit `--club` > `current_club` > usage error. Multi-club: `--all` (every managed club)
  * > explicit `--club a,b` > `current_club` > usage error. `--all` together with explicit `--club` is rejected (the two
  * express conflicting intent, so combining them is treated as a mistake rather than silently letting one win). A
  * failure carries exit code 2 (a usage error).
  *
  * `--club-id <id>` names a club outright, so it is rejected alongside anything else that names one: an id and a name
  * are two answers that can disagree, and the server must never have to pick between them (ADR 0016).
  */
object ClubResolver {

  val NoClubError =
    "no club specified: pass --club <slug> or set a current club with `ccas use-club <slug>`"
  val NoManagedError = "no managed clubs; add one with `ccas club add`"
  val BothError      = "--all and --club are mutually exclusive; pass one or the other"
  val ClubIdError    = "--club-id and --club are mutually exclusive; the id already names the club"
  val ClubIdAllError = "--club-id names one club; pass it on its own, not with --all"

  def single(
    explicit: Option[String],
    clubIdOption: Option[Long],
    currentClubOption: Option[String]
  ): IO[CliError, ClubTarget] =
    // Clean each source independently before falling back, so a blank explicit `--club` falls back to current_club
    // rather than blanking it out — mirrors how the comma-split multi path drops blank entries.
    (blankToNone(explicit), clubIdOption) match {
      case (Some(_), Some(_)) => ZIO.fail(CliError(ClubIdError, 2))
      case (Some(slug), None) => ZIO.succeed(bySlug(slug))
      case (None, Some(id))   => ZIO.succeed(ClubTarget.byId(ClubId(id)))
      case (None, None) =>
        blankToNone(currentClubOption) match {
          case Some(raw) => ZIO.succeed(fromCurrent(raw))
          case None      => ZIO.fail(CliError(NoClubError, 2))
        }
    }

  // Labels are the normalised name rather than the text as typed, so a `--club Team-Alpha` reads back the way every
  // other mention of the club does.
  private def bySlug(raw: String): ClubTarget = {
    val slug = ClubSlug(raw.trim)
    ClubTarget(ClubQuery.BySlug(slug), ClubSlug.unwrap(slug))
  }

  // A pointer that knows its club's id targets by id and keeps the stored slug as the label; one that doesn't is
  // nothing more than a name the user typed once.
  private def fromCurrent(raw: String): ClubTarget = {
    val ref = CurrentClubRef.parse(raw)
    ref.clubIdOption match {
      case Some(clubId) => ClubTarget(ClubQuery.ById(clubId), ClubSlug.unwrap(ClubSlug(ref.slug.trim)))
      case None         => bySlug(ref.slug)
    }
  }

  private def blankToNone(o: Option[String]): Option[String] = o.map(_.trim).filter(_.nonEmpty)

  def multi(
    fetchManaged: => Task[List[String]],
    explicit: List[String],
    clubIdOption: Option[Long],
    all: Boolean,
    currentClubOption: Option[String]
  ): Task[NonEmptyChunk[ClubTarget]] =
    clubIdOption match {
      case Some(_) if all                   => ZIO.fail(CliError(ClubIdAllError, 2))
      case Some(_) if explicit.nonEmpty     => ZIO.fail(CliError(ClubIdError, 2))
      case Some(clubId)                     => ZIO.succeed(NonEmptyChunk.single(ClubTarget.byId(ClubId(clubId))))
      case None if all && explicit.nonEmpty => ZIO.fail(CliError(BothError, 2))
      case None if all                      => fetchManaged.flatMap(toTargets(_, CliError(NoManagedError, 2)))
      case None if explicit.nonEmpty        => toTargets(explicit, CliError(NoClubError, 2))
      case None                             => currentTargets(currentClubOption)
    }

  // A current-club pointer is one target, and it is not a slug list, so it skips the trim-and-drop path entirely.
  private def currentTargets(currentClubOption: Option[String]): IO[CliError, NonEmptyChunk[ClubTarget]] =
    blankToNone(currentClubOption) match {
      case Some(raw) => ZIO.succeed(NonEmptyChunk.single(fromCurrent(raw)))
      case None      => ZIO.fail(CliError(NoClubError, 2))
    }

  // Trim and drop blanks so a padded/empty slug (from --club or the managed listing) can't reach the API path
  // verbatim — `ClubSlug.normalize` only lowercases.
  private def toTargets(slugs: List[String], ifEmpty: => CliError): IO[CliError, NonEmptyChunk[ClubTarget]] =
    ZIO
      .fromOption(NonEmptyChunk.fromIterableOption(slugs.map(_.trim).filter(_.nonEmpty).map(bySlug)))
      .orElseFail(ifEmpty)
}
