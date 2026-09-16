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

  /** Decide how (if at all) to refresh `current_club` after a job submit resolved a club server-side. Pure so the
    * write-back heuristic is testable without the dispatcher.
    *
    *   - `stored` — the raw `current_club` value at command start (`None` if unset).
    *   - `targetHasId` — whether the slug we submitted already carried an id (a `current_club` in `<id>:<slug>` form).
    *   - `targetSlug` — the slug we submitted.
    *   - `resolvedOption` — the club the server resolved the submit to (`None` when it did not resolve).
    *
    * Returns the new ref to persist, or `None` to leave `current_club` untouched. It writes only when the submit was
    * FOR the current club AND the canonical differs from what's stored (a Chess.com rename, or an id we didn't have
    * yet). "For the current club" is decided by id when the pointer carries one (rename-proof), else by matching the
    * submitted slug — so a bare command or an explicit `--club` naming the current club's slug both refresh it (the
    * latter is an intended id backfill, not an accident: it still names the same club).
    */
  def refreshedRef(
    stored: Option[String],
    targetHasId: Boolean,
    targetSlug: String,
    resolvedOption: Option[ClubRef]
  ): Option[CurrentClubRef] =
    (stored, resolvedOption) match {
      case (Some(raw), Some(resolved)) =>
        val ref       = parse(raw)
        val isCurrent = ref.clubIdOption.contains(resolved.clubId) || (!targetHasId && sameSlug(ref.slug, targetSlug))
        val next      = CurrentClubRef(Some(resolved.clubId), ClubSlug.unwrap(resolved.slug))
        Option.when(isCurrent && next.render != raw.trim)(next)
      case _ => None
    }

  /** The club a submit resolved to, when it is one the pointer may be repointed at. A [[ClubResolution.Moved]] name
    * now belongs to someone else's club: the job runs there because that is the name that was asked for, but a
    * pointer stored as a bare slug would otherwise follow the name and silently change which club it means.
    */
  def refreshTarget(resolution: ClubResolution): Option[ClubRef] = resolution match {
    case ClubResolution.Moved(_, _, _) => None
    case settled                       => settled.runnable.toOption
  }

  /** Slugs compare case-insensitively and trimmed: `ClubSlug.normalize` lowercases but does not trim. */
  private[cli] def sameSlug(a: String, b: String): Boolean = a.trim.equalsIgnoreCase(b.trim)
}
