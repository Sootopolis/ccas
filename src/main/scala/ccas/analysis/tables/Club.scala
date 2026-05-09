package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.{Task, ZIO}

import ccas.analysis.apps.ref.RefHelpers
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO, withTransaction}

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class Club(
  @Id clubId: ClubId,
  created: Instant,
  slug: ClubSlug,
  name: String,
  membersCount: Option[Int],
  latestMatchAt: Option[Instant],
  fetchedAt: Option[Instant]
) derives DbCodec {

  /** True when the slug is a tombstone placeholder set by `Club.resolveStaleSlug` (for clubs whose fresh slug couldn't
    * be discovered via match refs). Callers iterating clubs for URL emission or display should filter these out.
    */
  def isTombstoned: Boolean = Club.isTombstoneSlug(slug)

  /** Display variant for tombstoned clubs so user-facing output doesn't leak the placeholder. */
  def displayName: String =
    if (isTombstoned) { s"<unknown club #${ClubId.unwrap(clubId)}>" } else { name }
}

object Club {
  private val repo = ImmutableRepo[Club, ClubId]

  private val stalePattern = "^_stale_\\d+$".r

  /** True when the given slug matches the tombstone format set by `Club.resolveStaleSlug`. Useful at display sites
    * that hold a `ClubSlug` value but no full `Club` row.
    */
  def isTombstoneSlug(s: ClubSlug): Boolean = stalePattern.matches(s.value)

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club (
              club_id          BIGINT PRIMARY KEY,
              created          TIMESTAMPTZ NOT NULL,
              slug             TEXT NOT NULL,
              name             TEXT NOT NULL,
              members_count    INT,
              latest_match_at  TIMESTAMPTZ,
              fetched_at       TIMESTAMPTZ
            )""".update.run()
      sql"CREATE UNIQUE INDEX IF NOT EXISTS club_slug_key ON club (slug)".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[Club]] =
    connectZIO(repo.findAll.toList)

  def selectId(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[Club]] =
    connectZIO(repo.findById(clubId))

  def selectBySlug(slug: ClubSlug): ZIO[PostgresClient, SQLException, Option[Club]] =
    connectZIO {
      sql"""SELECT club_id, created, slug, name, members_count, latest_match_at, fetched_at
            FROM club WHERE slug = $slug""".query[Club].run().headOption
    }

  /** Returns the subset of `slugs` that exist in the `club` table. One round-trip via `WHERE slug = ANY(...)` —
    * preferred over per-slug `selectBySlug` calls for callers that need bulk membership testing.
    */
  def selectExistingSlugs(slugs: Set[ClubSlug]): ZIO[PostgresClient, SQLException, Set[ClubSlug]] =
    if (slugs.isEmpty) { ZIO.succeed(Set.empty) }
    else {
      connectZIO {
        val slugList = slugs.toList
        sql"SELECT slug FROM club WHERE slug = ANY($slugList)".query[ClubSlug].run().toSet
      }
    }

  /** Upserts a club. NB: `latest_match_at` and `fetched_at` are intentionally not updated on conflict — they are
    * managed separately by [[ccas.analysis.apps.clubdata.ClubDataApp]] via [[updateLatestMatchAt]] and
    * [[updateFetchedAt]] so other callers don't accidentally clobber the cached values with `None`.
    */
  def upsert(club: Club): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club (club_id, created, slug, name, members_count, latest_match_at, fetched_at)
            VALUES (${club.clubId}, ${club.created}, ${club.slug}, ${club.name}, ${club.membersCount}, ${club.latestMatchAt}, ${club.fetchedAt})
            ON CONFLICT (club_id) DO UPDATE SET
              slug = EXCLUDED.slug,
              name = EXCLUDED.name,
              members_count = EXCLUDED.members_count""".update.run()
    }

  /** Updates only the cached match-activity timestamp. Use this from ClubDataApp. */
  def updateLatestMatchAt(clubId: ClubId, latestMatchAt: Option[Instant]): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"UPDATE club SET latest_match_at = $latestMatchAt WHERE club_id = $clubId".update.run()
    }

  /** Stamps a club as successfully refreshed by ClubDataApp. Used by the `--min-age [hours]` filter on subsequent runs
    * to decide whether to skip this club.
    */
  def updateFetchedAt(clubId: ClubId, at: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"UPDATE club SET fetched_at = $at WHERE club_id = $clubId".update.run()
    }

  /** Upsert that handles slug conflicts by resolving the stale club's current slug via match ref.
    *
    * When another club already holds the target slug in the database, this method looks up one of the stale club's
    * matches and fetches the team URL from the Chess.com API to discover its current slug. Falls back to a placeholder
    * if the stale club has no matches.
    */
  def upsertResolvingSlugConflict(club: Club, client: ChessComClient): ZIO[PostgresClient, Throwable, Int] =
    withTransaction {
      for {
        existing <- selectBySlug(club.slug)
        _ <- existing match {
          case Some(stale) if stale.clubId != club.clubId => resolveStaleSlug(stale, client)
          case _                                          => ZIO.unit
        }
        result <- upsert(club)
      } yield result
    }

  /** Looks up a [[ClubMatchRef]] for the given club — explicit row first, otherwise inferred from `club_match` and
    * promoted via [[ClubMatchRef.findOrInfer]] — and reads the club's current slug from the corresponding team URL on
    * the Chess.com match endpoint. Used both to resolve slug collisions ([[resolveStaleSlug]]) and to recover from
    * rename-404s in ClubDataApp. Returns `None` if neither `club_match_ref` nor `club_match` carries the club.
    */
  def slugFromMatchRef(
    clubId: ClubId,
    client: ChessComClient
  ): ZIO[PostgresClient, Throwable, Option[ClubSlug]] =
    ClubMatchRef.findOrInfer(clubId).flatMap {
      case Some(ref) => fetchCurrentSlug(ref, client)
      case None      => ZIO.none
    }

  private def fetchCurrentSlug(ref: ClubMatchRef, client: ChessComClient): Task[Option[ClubSlug]] =
    RefHelpers.fetchTeamMatchTeams(client, ref.matchId, ref.isLive).map { teams =>
      val team = if (ref.isTeam1) { teams.team1 } else { teams.team2 }
      team.`@id`.path.segments.lastOption.map(ClubSlug.wrap)
    }

  private def resolveStaleSlug(stale: Club, client: ChessComClient): ZIO[PostgresClient, Throwable, Unit] =
    slugFromMatchRef(stale.clubId, client).flatMap {
      case Some(newSlug) =>
        connectZIO {
          sql"UPDATE club SET slug = $newSlug WHERE club_id = ${stale.clubId}".update.run()
        }.unit
      case None =>
        val placeholder = ClubSlug.wrap(s"_stale_${ClubId.unwrap(stale.clubId)}")
        connectZIO {
          sql"UPDATE club SET slug = $placeholder WHERE club_id = ${stale.clubId}".update.run()
        }.unit
    }

  /** Builds a [[Club]] from an [[ApiClub]] response. The slug is passed separately because callers may have a more
    * authoritative slug than the URL on the API response (e.g., from a slug conflict resolution). `latestMatchAt` and
    * `fetchedAt` are left as `None` — they are populated separately by ClubDataApp and preserved by [[upsert]].
    */
  def fromApi(apiClub: ApiClub, slug: ClubSlug): Club =
    Club(
      apiClub.clubId, Instant.ofEpochSecond(apiClub.created), slug, apiClub.name, Some(apiClub.membersCount), None, None
    )

  /** Same `latest_match_at` / `fetched_at` semantics as [[upsert]]: not touched on update — managed by ClubDataApp. */
  def upsertBatch(clubs: Iterable[Club]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(clubs) { club =>
        sql"""INSERT INTO club (club_id, created, slug, name, members_count, latest_match_at, fetched_at)
              VALUES (${club.clubId}, ${club.created}, ${club.slug}, ${club.name}, ${club.membersCount}, ${club.latestMatchAt}, ${club.fetchedAt})
              ON CONFLICT (club_id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                members_count = EXCLUDED.members_count""".update
      }
    }
}
