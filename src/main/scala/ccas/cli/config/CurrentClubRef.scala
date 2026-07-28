package ccas.cli.config

import ccas.api.misc.subtypes.ClubId

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
final case class CurrentClubRef(clubId: Option[ClubId], slug: String)

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

  /** Render for storage: `"<id>:<slug>"` when an id is known, else the bare slug. */
  def render(clubId: Option[ClubId], slug: String): String =
    clubId match {
      case Some(id) => s"${ClubId.unwrap(id)}:$slug"
      case None     => slug
    }

  /** Decide how (if at all) to refresh `current_club` after a job submit resolved a club server-side. Pure so the
    * write-back heuristic is testable without the dispatcher.
    *
    *   - `stored` — the raw `current_club` value at command start (`None` if unset).
    *   - `targetHasId` — whether the slug we submitted already carried an id (a `current_club` in `<id>:<slug>` form).
    *   - `targetSlug` — the slug we submitted.
    *   - `canonicalId` / `canonicalSlug` — the server-resolved club's stable id and current slug (`None` on a miss).
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
    canonicalId: Option[Long],
    canonicalSlug: Option[String]
  ): Option[CurrentClubRef] =
    (stored, canonicalId, canonicalSlug) match {
      case (Some(raw), Some(id), Some(canon)) =>
        val ref = parse(raw)
        val isCurrent =
          ref.clubId.exists(cur => ClubId.unwrap(cur) == id) || (!targetHasId && sameSlug(ref.slug, targetSlug))
        val next = CurrentClubRef(Some(ClubId.wrap(id)), canon)
        Option.when(isCurrent && render(next.clubId, next.slug) != raw.trim)(next)
      case _ => None
    }

  private def sameSlug(a: String, b: String): Boolean = a.trim.equalsIgnoreCase(b.trim)
}
