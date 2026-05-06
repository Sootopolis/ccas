package ccas.analysis.apps

import zio.{RIO, ZIO}

import ccas.analysis.tables.Club
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.client.{ChessComClient, HttpStatusException, onNotFound}
import ccas.utils.sql.PostgresClient

/** Resolves the current canonical slug for a club whose previously-known slug 404s on Chess.com.
  *
  * Strategy:
  *
  *  - **Tier A (DB lookup)** — never fires HTTP. With a `clubIdHint`, returns `Club.selectId(hint).slug` if the row's
  *    slug differs from the stale input (some other path already learned the new slug). Without a hint, Tier A is a
  *    no-op since `Club` has no historical slug table.
  *
  *  - **Tier B (match-ref endpoint)** — when Tier A returns `None` AND `clubIdHint` is `Some`, delegates to the
  *    existing `Club.slugFromMatchRef`, which fetches a `ClubMatchRef` board's team URL and extracts the slug.
  *
  * Verification fetches `ApiClub` for the candidate slug; on 404 the resolver returns `None` so the caller's original
  * 404 propagates.
  *
  * **Tombstone handling.** Slugs of the form `_stale_<clubId>` (set by `Club.resolveStaleSlug`) are skipped — never
  * returned as a "fresh" slug. Format collision risk is tracked in
  * [Sootopolis/ccas#21](https://github.com/Sootopolis/ccas/issues/21).
  */
object ClubSlugRenameResolver {

  /** Delegates to [[Club.isTombstoneSlug]] — single source of truth for the tombstone format. */
  def isTombstone(s: ClubSlug): Boolean = Club.isTombstoneSlug(s)

  def stalePlaceholder(clubId: ClubId): ClubSlug =
    ClubSlug.wrap(s"_stale_${ClubId.unwrap(clubId)}")

  /** Returns the current canonical slug when `staleSlug` 404s, or `None` if no rename can be inferred. Verifies the
    * candidate via `ApiClub` and does NOT update the `club` table. Use [[resolveAndPersist]] for the side-effecting
    * variant.
    */
  def resolveCurrentSlug(
    client: ChessComClient,
    staleSlug: ClubSlug,
    clubIdHint: Option[ClubId]
  ): RIO[PostgresClient, Option[ClubSlug]] =
    resolveAndPersist(client, staleSlug, clubIdHint).map(_.map(_._1))

  /** Resolves the current slug AND persists it via `Club.upsertResolvingSlugConflict`. Returns the verified `ApiClub`
    * so callers don't have to refetch.
    */
  def resolveAndPersist(
    client: ChessComClient,
    staleSlug: ClubSlug,
    clubIdHint: Option[ClubId]
  ): RIO[PostgresClient, Option[(ClubSlug, ApiClub)]] =
    resolveCandidate(client, staleSlug, clubIdHint).flatMap {
      case None => ZIO.none
      case Some(candidate) =>
        verify(client, candidate, clubIdHint).flatMap {
          case None => ZIO.none
          case Some((fresh, apiClub)) =>
            Club.upsertResolvingSlugConflict(Club.fromApi(apiClub, fresh), client).as(Some((fresh, apiClub)))
        }
    }

  private def tierADb(staleSlug: ClubSlug, clubIdHint: Option[ClubId]): RIO[PostgresClient, Option[ClubSlug]] =
    clubIdHint match {
      case None => ZIO.none
      case Some(hint) =>
        Club.selectId(hint).map(_.flatMap { current =>
          if (current.slug != staleSlug && !isTombstone(current.slug)) { Some(current.slug) }
          else { None }
        })
    }

  private def tierBMatchRef(
    client: ChessComClient,
    clubIdHint: Option[ClubId],
    staleSlug: ClubSlug
  ): RIO[PostgresClient, Option[ClubSlug]] = {
    val effect: RIO[PostgresClient, Option[ClubSlug]] = clubIdHint match {
      case None => ZIO.none
      case Some(hint) =>
        Club.slugFromMatchRef(hint, client).map(_.filter(s => s != staleSlug && !isTombstone(s)))
    }
    effect.catchAll {
      // HTTP errors on the match-ref endpoint are expected (cancelled match, intermittent 5xx). Swallow so the
      // caller's original 404 propagates. Other errors (DB, decode) get a debug log but still return None to avoid
      // masking the original failure.
      case _: HttpStatusException => ZIO.none
      case e =>
        ZIO.logDebug(s"  Tier B slug recovery internal error for $staleSlug: ${e.getMessage}").as(None)
    }
  }

  /** Resolves a candidate fresh slug. Errors are swallowed (debug-logged when non-HTTP) so the caller's original 404
    * is never replaced with a recovery-internal failure.
    */
  private def resolveCandidate(
    client: ChessComClient,
    staleSlug: ClubSlug,
    clubIdHint: Option[ClubId]
  ): RIO[PostgresClient, Option[ClubSlug]] =
    deriveHint(staleSlug, clubIdHint).flatMap { effectiveHint =>
      tierADb(staleSlug, effectiveHint).flatMap {
        case some @ Some(_) => ZIO.succeed(some)
        case None           => tierBMatchRef(client, effectiveHint, staleSlug)
      }
    }.catchAll {
      case _: HttpStatusException => ZIO.none
      case e =>
        ZIO.logDebug(s"  Slug resolver internal error for $staleSlug: ${e.getMessage}").as(None)
    }

  /** Derives a `clubIdHint` from the stale slug when the caller didn't supply one. Looks up our `club` table by the
    * stale slug — if we have a row, we know which club_id this rename is about. Eliminates per-callsite boilerplate
    * (callers can pass `None` and let the resolver discover the hint).
    */
  private def deriveHint(
    staleSlug: ClubSlug,
    clubIdHint: Option[ClubId]
  ): RIO[PostgresClient, Option[ClubId]] =
    clubIdHint match {
      case some @ Some(_) => ZIO.succeed(some)
      case None           => Club.selectBySlug(staleSlug).map(_.map(_.clubId))
    }

  private def verify(
    client: ChessComClient,
    candidate: ClubSlug,
    clubIdHint: Option[ClubId]
  ): RIO[Any, Option[(ClubSlug, ApiClub)]] =
    ApiClub.get(client, candidate).map { apiClub =>
      val matches = clubIdHint.forall(_ == apiClub.clubId)
      // Read the slug from `@id`'s last path segment so we get whatever Chess.com normalized to (the candidate slug
      // might differ from the canonical one returned in the response).
      Option.when(matches)((ClubSlug.wrap(apiClub.`@id`.path.segments.last), apiClub))
    }.onNotFound(_ => ZIO.none)
}

/** Combinator: wrap any 404-prone effect targeting `/pub/club/{slug}/...` with rename recovery. On 404, the resolver
  * is invoked; on success the caller's `retryWith(freshSlug)` runs. Other errors propagate.
  */
extension [R, A](self: ZIO[R, Throwable, A])
  def withClubSlugRenameRecovery(
    client: ChessComClient,
    stale: ClubSlug,
    clubIdHint: Option[ClubId]
  )(retryWith: ClubSlug => ZIO[R, Throwable, A])
    : ZIO[R & PostgresClient, Throwable, A] =
    self.onNotFound { e =>
      ClubSlugRenameResolver.resolveAndPersist(client, stale, clubIdHint).flatMap {
        case Some((fresh, _)) =>
          ZIO.logInfo(s"  Slug rename recovered: $stale → $fresh; retrying") *> retryWith(fresh)
        case None => ZIO.fail(e)
      }
    }
