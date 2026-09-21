package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import scala.util.chaining.*

import com.augustnagro.magnum.*
import zio.{Task, ZIO}

import ccas.analysis.apps.ref.RefHelpers
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.client.{ChessComClient, FetchResult}
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

  private[tables] val selectCols =
    SqlLiteral("club_id, created, slug, name, members_count, latest_match_at, fetched_at")

  private val staleRegex   = "^_stale_[0-9]+$"
  private val stalePattern = staleRegex.r

  private def tombstoneSlug(clubId: ClubId): ClubSlug = ClubSlug.wrap(s"_stale_${ClubId.unwrap(clubId)}")

  /** [[isTombstoneSlug]] as a SQL regex literal for a `!~` match, so the tombstone format has one home. */
  private[tables] val TombstoneSlugRegex: SqlLiteral = SqlLiteral(s"'$staleRegex'")

  /** True when the given slug matches the tombstone format set by `Club.resolveStaleSlug`. Useful at display sites
    * that hold a `ClubSlug` value but no full `Club` row.
    */
  def isTombstoneSlug(s: ClubSlug): Boolean = stalePattern.matches(s.value)

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      sql"""CREATE TABLE IF NOT EXISTS club (
              club_id          BIGINT PRIMARY KEY,
              created          TIMESTAMPTZ NOT NULL,
              slug             TEXT NOT NULL,
              name             TEXT NOT NULL,
              members_count    INT,
              latest_match_at  TIMESTAMPTZ,
              fetched_at       TIMESTAMPTZ
            )""".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[Club]] =
    connectZIO(repo.findAll.toList)

  def selectId(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[Club]] =
    connectZIO(repo.findById(clubId))

  /** Upserts a club. NB: `latest_match_at` and `fetched_at` are intentionally not updated on conflict — they are
    * managed separately by [[ccas.analysis.apps.clubdata.ClubDataApp]] via [[updateLatestMatchAt]] and
    * [[updateFetchedAt]] so other callers don't accidentally clobber the cached values with `None`.
    */
  def upsert(club: Club): ZIO[PostgresClient, SQLException, Int] =
    transactZIO(upsertFrag(club).run().tap(_ => recordName(club)))

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
    * if the stale club has no matches. `club.slug` carries no unique index, so the conflict is one the tombstones
    * keep rather than one the database raises (#254).
    */
  def upsertResolvingSlugConflict(club: Club, client: ChessComClient): ZIO[PostgresClient, Throwable, Int] =
    withTransaction {
      for {
        existing <- ClubName.selectCurrentHolder(club.slug)
        _        <- ZIO.foreachDiscard(existing.filter(_.clubId != club.clubId))(resolveStaleSlug(_, client))
        result   <- upsert(club)
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
    slugFromMatchRefResult(clubId, client).flatMap {
      case Some(result) => result.foldPresentZIO(_.getValue, _.getValue)
      case None         => ZIO.none
    }

  /** [[slugFromMatchRef]], but exposing the fetch outcome as a value instead of committing to "fail on absence" —
    * used by `ClubSlugRenameResolver.tierBMatchRef`, whose answer to absence is "try Tier C," not "fail."
    */
  def slugFromMatchRefResult(
    clubId: ClubId,
    client: ChessComClient
  ): ZIO[PostgresClient, Throwable, Option[FetchResult[Option[ClubSlug]]]] =
    ClubMatchRef.findOrInfer(clubId).flatMap {
      case Some(ref) => fetchCurrentSlugResult(ref, client).asSome
      case None      => ZIO.none
    }

  private def fetchCurrentSlugResult(ref: ClubMatchRef, client: ChessComClient): Task[FetchResult[Option[ClubSlug]]] =
    RefHelpers.fetchTeamMatchTeamsResult(client, ref.matchId, ref.isLive).map(_.map { teams =>
      val team = if (ref.isTeam1) { teams.team1 } else { teams.team2 }
      ClubSlug.fromUrlOption(team.`@id`)
    })

  private def resolveStaleSlug(stale: Club, client: ChessComClient): ZIO[PostgresClient, Throwable, Int] =
    slugFromMatchRef(stale.clubId, client).flatMap { newSlugOption =>
      val slug = newSlugOption.getOrElse(tombstoneSlug(stale.clubId))
      transactZIO {
        sql"UPDATE club SET slug = $slug WHERE club_id = ${stale.clubId}".update.run()
          .tap(_ => recordName(stale.copy(slug = slug)))
      }
    }

  /** Builds a [[Club]] from an [[ApiClub]] response, reading the slug from `apiClub.canonicalSlug` — the name
    * Chess.com answers to now, not necessarily the one the caller requested. `latestMatchAt` and `fetchedAt` are
    * left as `None` — they are populated separately by ClubDataApp and preserved by [[upsert]].
    */
  def fromApi(apiClub: ApiClub): Club =
    Club(
      apiClub.clubId, Instant.ofEpochSecond(apiClub.created), apiClub.canonicalSlug, apiClub.name,
      Some(apiClub.membersCount), None, None
    )

  private def upsertFrag(club: Club): Update =
    sql"""INSERT INTO club (club_id, created, slug, name, members_count, latest_match_at, fetched_at)
          VALUES (${club.clubId}, ${club.created}, ${club.slug}, ${club.name}, ${club.membersCount}, ${club.latestMatchAt}, ${club.fetchedAt})
          ON CONFLICT (club_id) DO UPDATE SET
            slug = EXCLUDED.slug,
            name = EXCLUDED.name,
            members_count = EXCLUDED.members_count""".update

  // Every write of `club.slug` goes through here, so `club_name` never drifts from it.
  private def recordName(club: Club)(using DbTx): Int =
    ClubName.record(club.clubId, Option.unless(isTombstoneSlug(club.slug))(club.slug))
}
