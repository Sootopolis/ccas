package ccas.analysis.apps

import zio.{RIO, ZIO}

import ccas.analysis.tables.{Club, ClubAdmin, Player}
import ccas.api.club.ApiClub
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Username}
import ccas.api.player.ApiPlayerClubs
import ccas.utils.client.{ChessComClient, NetworkUnavailableException, ReportedNotFound, onNotFound, swallowRecoveryErrors}
import ccas.utils.sql.PostgresClient

/** Resolves the current canonical slug for a club whose previously-known slug 404s on Chess.com.
  *
  *  - **Tier A (DB lookup)** — never fires HTTP: `Club.selectId(hint).slug`, when it differs from the stale input.
  *    A no-op without a hint, since `Club` keeps no historical slug table.
  *  - **Tier B (match-ref endpoint)** — `Club.slugFromMatchRef`, which reads a `ClubMatchRef` board's team URL.
  *  - **Tier C (admin clubs)** — fetches each stored `ClubAdmin`'s `/pub/player/{username}/clubs` and looks for a
  *    slug whose `ApiClub.clubId` matches the hint. Bounded by the admin count, short-circuits on first hit, and
  *    closes the gap for a club that has never played a match.
  *
  * Verification fetches `ApiClub` for the candidate; on 404 the caller's original 404 propagates. Tombstoned slugs
  * (`_stale_<clubId>`) are never returned as fresh.
  *
  * Why the entry points gate on [[ccas.utils.client.ReportedNotFound]] rather than any 404 — it is what stops Tier
  * C's fan-out firing on noise: `docs/adr/0010-rename-recovery-for-usernames-and-club-slugs.md`.
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

  /** Convenience: fetches `/pub/club/{slug}` and falls back to slug-rename recovery on 404. Returns the verified
    * (post-recovery) `ApiClub` paired with the canonical slug — saves callers a re-fetch when they need both. On 404
    * with no rename inferred, the original 404 propagates. Mirrors [[UsernameRenameResolver.fetchOrRecover]] for the
    * club path.
    */
  def fetchOrRecover(
    client: ChessComClient,
    slug: ClubSlug,
    clubIdHint: Option[ClubId] = None
  ): RIO[PostgresClient, (ApiClub, ClubSlug)] =
    ApiClub.get(client, slug).map(_ -> slug).catchSome { case e: ReportedNotFound =>
      resolveAndPersist(client, slug, clubIdHint).flatMap {
        case Some((freshSlug, apiClub)) => ZIO.succeed(apiClub -> freshSlug)
        case None                       => ZIO.fail(e)
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
    effect.swallowRecoveryErrors(s"Tier B slug recovery for $staleSlug")
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
      ZIO.collectFirst(
        List[RIO[PostgresClient, Option[ClubSlug]]](
          tierADb(staleSlug, effectiveHint),
          tierBMatchRef(client, effectiveHint, staleSlug),
          tierCAdminClubs(client, effectiveHint, staleSlug)
        )
      )(identity)
    }.swallowRecoveryErrors(s"slug resolver for $staleSlug")

  /** Tier C: fans out across stored `ClubAdmin` rows for the hint, querying each admin's `/pub/player/{u}/clubs`
    * and looking for a slug whose verified `ApiClub.clubId` matches the hint. Slugs already known to our `Club` table
    * are skipped (they're guaranteed-not-the-rename, else Tier A would have caught it). Errors are swallowed and
    * debug-logged to preserve the caller's original 404.
    */
  private def tierCAdminClubs(
    client: ChessComClient,
    clubIdHint: Option[ClubId],
    staleSlug: ClubSlug
  ): RIO[PostgresClient, Option[ClubSlug]] = {
    val effect: RIO[PostgresClient, Option[ClubSlug]] = clubIdHint match {
      case None => ZIO.none
      case Some(hint) =>
        for {
          adminRows    <- ClubAdmin.selectByClub(hint)
          adminPlayers <- Player.selectByIds(adminRows.map(_.playerId))
          // Surface DB drift (admin row references a player_id with no `player` row) so it doesn't masquerade as a
          // silently-shrunken admin set.
          _ <- ZIO.logDebug(
            s"  Tier C: ${adminRows.size - adminPlayers.size} admin row(s) for clubId=$hint had no Player row"
          ).when(adminPlayers.size != adminRows.size)
          // Skip closed/banned admins: their /clubs can't contain `hint`. Their `club_admin` row persists from a prior
          // refresh; `Player.status` was updated since via another code path (Membership/Recruitment/History).
          usernames = adminPlayers.collect {
            case p if !p.isTombstoned && p.status == PlayerStatusCategory.Active => p.username
          }
          result <- ZIO.collectFirst(usernames)(adminLookup(client, _, hint, staleSlug))
          _ <- ZIO.foreachDiscard(result)(fresh =>
            ZIO.logInfo(s"  Tier C slug recovery hit via admin lookup: $staleSlug → $fresh")
          )
        } yield result
    }
    effect.swallowRecoveryErrors(s"Tier C slug recovery for $staleSlug")
  }

  /** Per-admin step of Tier C: fetch the admin's clubs list, filter to slugs we don't already know, and verify
    * candidates against `clubIdHint` via `ApiClub`. Per-admin failures (404, decode) return `None` so the outer
    * `collectFirst` advances to the next admin instead of aborting the whole tier.
    */
  private def adminLookup(
    client: ChessComClient,
    username: Username,
    hint: ClubId,
    staleSlug: ClubSlug
  ): RIO[PostgresClient, Option[ClubSlug]] = {
    val effect = for {
      apiClubs <- client.getUncached[ApiPlayerClubs](ApiPlayerClubs.getUrl(username))
      candidates = apiClubs.clubs.map(_.clubName).distinct.filter(s => s != staleSlug && !isTombstone(s))
      knownSlugs <- Club.selectExistingSlugs(candidates.toSet)
      unknown = candidates.filterNot(knownSlugs.contains).toList
      result <- ZIO.collectFirst(unknown)(verifyClubIdMatch(client, _, hint))
    } yield result
    effect.swallowRecoveryErrors(s"Tier C admin lookup for $username")
  }

  /** Fetches `ApiClub` for `slug` and returns `Some(slug)` only if its `clubId` matches `hint`. 404s become `None`
    * so the iterator moves on instead of failing.
    */
  private def verifyClubIdMatch(
    client: ChessComClient,
    slug: ClubSlug,
    hint: ClubId
  ): RIO[Any, Option[ClubSlug]] =
    ApiClub.get(client, slug)
      .map(c => Option.when(c.clubId == hint)(slug))
      .onNotFound(_ => ZIO.none)

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

  /** Resolves a club slug to its ID: `Some(clubId)` if known locally, else fetch from Chess.com and persist.
    *
    * Expected errors (404, decode, SQL) swallow to `None`, matching the `Club.resolveOrFetch` semantics this
    * supersedes; a systemic [[NetworkUnavailableException]] re-raises so a caller's retry loop aborts cleanly rather
    * than recording a bogus skip (#119). Lives in apps/ rather than tables/ to avoid a cycle with `tables.Club`.
    *
    * Slug-rename recovery is deliberately not wired here — it could not fire. See
    * `docs/adr/0010-rename-recovery-for-usernames-and-club-slugs.md`.
    */
  def resolveOrFetch(
    client: ChessComClient,
    slug: ClubSlug
  ): RIO[PostgresClient, Option[ClubId]] =
    Club.selectBySlug(slug).flatMap {
      case Some(club) => ZIO.some(club.clubId)
      case None =>
        (for {
          apiClub <- ApiClub.get(client, slug)
          canonical = ClubSlug.wrap(apiClub.`@id`.path.segments.last)
          _ <- Club.upsertResolvingSlugConflict(Club.fromApi(apiClub, canonical), client)
        } yield Option(apiClub.clubId))
          .tapError(e => ZIO.logDebug(s"  ClubSlugRenameResolver.resolveOrFetch $slug failed: ${e.getMessage}"))
          .catchAll(e => NetworkUnavailableException.recoverUnless(e)(ZIO.none))
    }
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
    self.catchSome { case e: ReportedNotFound =>
      ClubSlugRenameResolver.resolveAndPersist(client, stale, clubIdHint).flatMap {
        case Some((fresh, _)) =>
          ZIO.logInfo(s"  Slug rename recovered: $stale → $fresh; retrying") *> retryWith(fresh)
        case None => ZIO.fail(e)
      }
    }
