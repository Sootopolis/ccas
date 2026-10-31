package ccas.cli.config

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}

/** The parsed form of the CLI config's `current_club` value. Stored as `"<id>:<slug>"` when the CLI knows the club's
  * stable Chess.com id, or a bare `"<slug>"` when it doesn't yet (hand-edited config, or a slug set while offline /
  * against a server that couldn't resolve it). The id is the authoritative target — it survives a Chess.com slug rename,
  * so a `current_club` that has been renamed still resolves — while the slug is a human-readable display label that the
  * server refreshes on the next successful command (#176).
  *
  * A club slug is a URL path segment (`[a-z0-9-]`), so it never contains a colon; the value is split on the FIRST colon
  * and the left side is treated as an id only when it is all digits. Anything else is a bare slug, which keeps existing
  * slug-only configs working unchanged and tolerates a hand-typed value.
  */
final case class CurrentClubRef(clubIdOption: Option[ClubId], slug: String) {

  /** Render for storage: `"<id>:<slug>"` when an id is known, else the bare slug. The inverse of [[CurrentClubRef.parse]]. */
  def render: String = clubIdOption match {
    case Some(id) => s"${ClubId.unwrap(id)}:$slug"
    case None     => slug
  }

  /** Whether a club a submit reported missing is this one, compared by whichever half the submit named it by: a
    * pointer that knows its id addressed the server with it, and a name this pointer also answers to is worth a word
    * to whoever typed it. Drives the stranded-`current_club` hint.
    */
  def names(query: ClubQuery): Boolean = query match {
    case ClubQuery.ById(clubId) => clubIdOption.contains(clubId)
    case ClubQuery.BySlug(slug) => CurrentClubRef.sameSlug(this.slug, ClubSlug.unwrap(slug))
  }

  /** Whether this pointer is `club`: by id when the pointer knows one, since a name can pass to another club; otherwise
    * by the name the club holds now, so a club holding none is never a bare pointer's. Marks the current club in
    * `club list`.
    */
  def pointsAt(club: ClubRef): Boolean = clubIdOption match {
    case Some(clubId) => clubId == club.clubId
    case None         => club.slugOption.exists(held => CurrentClubRef.sameSlug(slug, ClubSlug.unwrap(held)))
  }

  /** Whether this pointer means `club`, which a command reached through `query`: the club it [[pointsAt]], or for a
    * bare pointer, the club reached by the name it was set with. Clearing the pointer when its club stops being managed
    * and refreshing it when a command reaches its club both ask this, so they agree on which club that is.
    */
  def means(club: ClubRef, query: ClubQuery): Boolean =
    pointsAt(club) || (clubIdOption.isEmpty && names(query))
}

object CurrentClubRef {

  /** Parse a raw `current_club` value. Never fails: an unparseable id prefix falls back to treating the whole value as
    * a slug, so a malformed pointer degrades to slug-only resolution rather than breaking the command.
    */
  def parse(raw: String): CurrentClubRef = {
    val trimmed = raw.trim
    trimmed.indexOf(':') match {
      case -1 => CurrentClubRef(None, trimmed)
      case i =>
        val (idPart, slugPart) = (trimmed.take(i), trimmed.drop(i + 1))
        idPart.toLongOption.filter(_ >= 0L) match {
          case Some(id) if slugPart.nonEmpty => CurrentClubRef(Some(ClubId.wrap(id)), slugPart)
          case _                             => CurrentClubRef(None, trimmed)
        }
    }
  }

  /** The pointer to persist after a command sent `query` and the server answered `resolution`, given the raw
    * `current_club` it started with: the `<id>:<slug>` of the club acted on, when that is the club the pointer
    * [[CurrentClubRef.means]] and it differs from what is stored — a Chess.com rename, or an id a bare pointer lacked.
    * A bare pointer is a name, so it follows one that has [[ClubResolution.Moved]]: the server has already recorded
    * the new holder, and the next command would follow it anyway (#386). `None` leaves `current_club` untouched.
    */
  def refreshedRef(
    stored: Option[String],
    query: ClubQuery,
    resolution: ClubResolution
  ): Option[CurrentClubRef] =
    (stored, resolution.runnable.toOption) match {
      case (Some(raw), Some(resolved)) =>
        val next = CurrentClubRef(Some(resolved.clubId), ClubSlug.unwrap(resolved.slug))
        Option.when(parse(raw).means(ClubRef.of(resolved), query) && next.render != raw.trim)(next)
      case _ => None
    }

  /** Slugs compare case-insensitively and trimmed: `ClubSlug.normalize` lowercases but does not trim. */
  private[cli] def sameSlug(a: String, b: String): Boolean = a.trim.equalsIgnoreCase(b.trim)
}
