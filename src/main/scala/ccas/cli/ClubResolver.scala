package ccas.cli

import zio.*

import ccas.analysis.apps.{ClubQuery, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.cli.config.CurrentClubRef

/** A club a command is targeting: how the server should look it up, and what to call it until it answers. `--club-id`,
  * a `current_club` that knows its club's id and the `--all` managed expansion send the stable id, which resolves
  * whatever the club has since been renamed to (#176, #255); a typed `--club <slug>` and a bare-slug `current_club` send
  * the name. The label is only ever for display: the name last seen where there is one, being friendlier than `#<id>`.
  */
final case class ClubTarget(query: ClubQuery, label: String)

object ClubTarget {

  /** A club named by its id, which is all the label can say until the server answers with a name. */
  def byId(clubId: ClubId): ClubTarget = {
    val query = ClubQuery.ById(clubId)
    ClubTarget(query, query.describe)
  }

  /** A club the server has just named, addressed by its id so a name that changes hands before the submit can't
    * redirect it, and labelled by the name it held then.
    */
  def of(club: NamedClub): ClubTarget = ClubTarget(ClubQuery.ById(club.clubId), ClubSlug.unwrap(club.slug))
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
  *
  * Some commands name a club without falling back to `current_club`: `schedule add`, whose job kinds don't all take a
  * club ([[optional]]), and `club add` / `remove` / `show`, whose club is the operand ([[operand]]).
  */
object ClubResolver {

  val NoClubError =
    "no club specified: pass --club <slug> or set a current club with `ccas use-club <slug>`"
  val NoManagedError = "no managed clubs; add one with `ccas club add`"
  val BothError      = "--all and --club are mutually exclusive; pass one or the other"
  val ClubIdError    = "--club-id and --club are mutually exclusive; the id already names the club"
  val ClubIdAllError = "--club-id names one club; pass it on its own, not with --all"
  val BlankClubError = "--club names no club; pass a slug, or leave --club out"
  val NoOperandError = "no club specified: pass its slug or --club-id <id>"
  val OperandIdError = "a slug and --club-id are mutually exclusive; the id already names the club"

  def single(
    explicit: Option[String],
    clubIdOption: Option[Long],
    currentClubOption: Option[String]
  ): IO[CliError, ClubTarget] =
    // Clean each source independently before falling back, so a blank explicit `--club` falls back to current_club
    // rather than blanking it out — mirrors how the comma-split multi path drops blank entries.
    named(explicit, clubIdOption).flatMap {
      case Some(target) => ZIO.succeed(target)
      case None =>
        blankToNone(currentClubOption) match {
          case Some(raw) => ZIO.succeed(fromCurrent(raw))
          case None      => ZIO.fail(CliError(NoClubError, 2))
        }
    }

  // With no current_club to fall back to, a blank `--club` is a mistake rather than an absent one: silently dropping it
  // would schedule a club-scoped kind against no club.
  def optional(explicit: Option[String], clubIdOption: Option[Long]): IO[CliError, Option[ClubTarget]] =
    if (explicit.exists(_.trim.isEmpty)) { ZIO.fail(CliError(BlankClubError, 2)) }
    else { named(explicit, clubIdOption) }

  private def named(explicit: Option[String], clubIdOption: Option[Long]): IO[CliError, Option[ClubTarget]] =
    (blankToNone(explicit), clubIdOption) match {
      case (Some(_), Some(_)) => ZIO.fail(CliError(ClubIdError, 2))
      case (Some(slug), None) => ZIO.some(bySlug(slug))
      case (None, Some(id))   => ZIO.some(ClubTarget.byId(ClubId(id)))
      case (None, None)       => ZIO.none
    }

  // More than one slug is usually an option written after the slug, which zio-cli swallows as another positional, so
  // the arity is checked here rather than trusted to the parser.
  def operand(slugs: List[String], clubIdOption: Option[Long]): IO[CliError, ClubTarget] =
    (slugs.map(_.trim).filter(_.nonEmpty), clubIdOption) match {
      case (Nil, None)         => ZIO.fail(CliError(NoOperandError, 2))
      case (Nil, Some(id))     => ZIO.succeed(ClubTarget.byId(ClubId(id)))
      case (slug :: Nil, None) => ZIO.succeed(bySlug(slug))
      case (_ :: Nil, Some(_)) => ZIO.fail(CliError(OperandIdError, 2))
      case (several, _) =>
        val listed = several.mkString(", ")
        ZIO.fail(CliError(s"expected one club slug, got ${several.size} ($listed); options go before the slug", 2))
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
    fetchManaged: => Task[List[NamedClub]],
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
      case None if all                      => fetchManaged.flatMap(managedTargets)
      case None if explicit.nonEmpty        => toTargets(explicit)
      case None                             => currentTargets(currentClubOption)
    }

  // A current-club pointer is one target, and it is not a slug list, so it skips the trim-and-drop path entirely.
  private def currentTargets(currentClubOption: Option[String]): IO[CliError, NonEmptyChunk[ClubTarget]] =
    blankToNone(currentClubOption) match {
      case Some(raw) => ZIO.succeed(NonEmptyChunk.single(fromCurrent(raw)))
      case None      => ZIO.fail(CliError(NoClubError, 2))
    }

  private def managedTargets(clubs: List[NamedClub]): IO[CliError, NonEmptyChunk[ClubTarget]] =
    ZIO.fromOption(NonEmptyChunk.fromIterableOption(clubs.map(ClubTarget.of))).orElseFail(CliError(NoManagedError, 2))

  // Trim and drop blanks so a padded/empty `--club` slug can't reach the API path verbatim — `ClubSlug.normalize` only
  // lowercases.
  private def toTargets(slugs: List[String]): IO[CliError, NonEmptyChunk[ClubTarget]] =
    ZIO
      .fromOption(NonEmptyChunk.fromIterableOption(slugs.map(_.trim).filter(_.nonEmpty).map(bySlug)))
      .orElseFail(CliError(NoClubError, 2))
}
