package ccas.cli.config

import ccas.analysis.apps.{ClubQuery, ClubResolution, NamedClub}
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

  /** Whether this pointer means `club`, which a command reached through `query`. By id when the pointer knows one,
    * since a name can pass to another club; otherwise by the name it was set with, which is either what was typed or
    * what the club is called now. Clearing the pointer when its club stops being managed and refreshing it when a
    * command reaches its club both ask this, so they agree on which club that is.
    */
  def means(club: NamedClub, query: ClubQuery): Boolean = clubIdOption match {
    case Some(clubId) => clubId == club.clubId
    case None         => CurrentClubRef.sameSlug(slug, ClubSlug.unwrap(club.slug)) || names(query)
  }
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

  /** The pointer to persist after a command sent `query` and the server resolved it to `resolvedOption`, given the
    * raw `current_club` it started with: the club's `<id>:<slug>`, when that is the club the pointer
    * [[CurrentClubRef.means]] and it differs from what is stored — a Chess.com rename, or an id a bare pointer lacked.
    * `None` leaves `current_club` untouched. Pure so the write-back is testable without the dispatcher.
    */
  def refreshedRef(
    stored: Option[String],
    query: ClubQuery,
    resolvedOption: Option[NamedClub]
  ): Option[CurrentClubRef] =
    (stored, resolvedOption) match {
      case (Some(raw), Some(resolved)) =>
        val next = CurrentClubRef(Some(resolved.clubId), ClubSlug.unwrap(resolved.slug))
        Option.when(parse(raw).means(resolved, query) && next.render != raw.trim)(next)
      case _ => None
    }

  /** The club a submit resolved to, when it is one the pointer may be repointed at. A [[ClubResolution.Moved]] name
    * now belongs to someone else's club: the job runs there because that is the name that was asked for, but a
    * pointer stored as a bare slug would otherwise follow the name and silently change which club it means.
    */
  def refreshTarget(resolution: ClubResolution): Option[NamedClub] = resolution match {
    case ClubResolution.Moved(_, _, _) => None
    case settled                       => settled.runnable.toOption
  }

  /** Slugs compare case-insensitively and trimmed: `ClubSlug.normalize` lowercases but does not trim. */
  private[cli] def sameSlug(a: String, b: String): Boolean = a.trim.equalsIgnoreCase(b.trim)
}
