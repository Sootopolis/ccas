package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

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
  latestMatchAt: Option[Instant]
) derives DbCodec

object Club {
  private val repo = ImmutableRepo[Club, ClubId]

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club (
              club_id          BIGINT PRIMARY KEY,
              created          TIMESTAMPTZ NOT NULL,
              slug             TEXT NOT NULL,
              name             TEXT NOT NULL,
              members_count    INT,
              latest_match_at  TIMESTAMPTZ
            )""".update.run()
      sql"CREATE UNIQUE INDEX IF NOT EXISTS club_slug_key ON club (slug)".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[Club]] =
    connectZIO(repo.findAll.toList)

  def selectId(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[Club]] =
    connectZIO(repo.findById(clubId))

  def selectBySlug(slug: ClubSlug): ZIO[PostgresClient, SQLException, Option[Club]] =
    connectZIO {
      sql"SELECT club_id, created, slug, name, members_count, latest_match_at FROM club WHERE slug = $slug"
        .query[Club].run().headOption
    }

  /** Upserts a club. NB: `latest_match_at` is intentionally not updated here — it is managed separately by
    * [[ccas.analysis.apps.clubdata.ClubDataApp]] via [[updateLatestMatchAt]] so other callers don't accidentally
    * clobber the cached value with `None`.
    */
  def upsert(club: Club): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club (club_id, created, slug, name, members_count, latest_match_at)
            VALUES (${club.clubId}, ${club.created}, ${club.slug}, ${club.name}, ${club.membersCount}, ${club.latestMatchAt})
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

  private def resolveStaleSlug(stale: Club, client: ChessComClient): ZIO[PostgresClient, Throwable, Unit] =
    ClubMatch.selectClubMatchRef(stale.clubId).flatMap {
      case Some(ref) =>
        RefHelpers.fetchTeamMatchTeams(client, ref.matchId, ref.isLive).flatMap { teams =>
          val team = if (ref.isTeam1) { teams.team1 }
          else { teams.team2 }
          val newSlug = ClubSlug.wrap(team.`@id`.path.segments.last)
          connectZIO {
            sql"UPDATE club SET slug = $newSlug WHERE club_id = ${stale.clubId}".update.run()
          }.unit
        }
      case None =>
        val placeholder = ClubSlug.wrap(s"_stale_${ClubId.unwrap(stale.clubId)}")
        connectZIO {
          sql"UPDATE club SET slug = $placeholder WHERE club_id = ${stale.clubId}".update.run()
        }.unit
    }

  /** Builds a [[Club]] from an [[ApiClub]] response. The slug is passed separately because callers may have a more
    * authoritative slug than the URL on the API response (e.g., from a slug conflict resolution). `latestMatchAt` is
    * left as `None` — it is populated separately by ClubDataApp and preserved by [[upsert]].
    */
  def fromApi(apiClub: ApiClub, slug: ClubSlug): Club =
    Club(apiClub.clubId, Instant.ofEpochSecond(apiClub.created), slug, apiClub.name, Some(apiClub.membersCount), None)

  /** Resolves a club slug to its ID, fetching from the Chess.com API and persisting if not already in the database. */
  def resolveOrFetch(client: ChessComClient, slug: ClubSlug): ZIO[PostgresClient, SQLException, Option[ClubId]] =
    selectBySlug(slug).flatMap {
      case Some(club) => ZIO.some(club.clubId)
      case None =>
        (for {
          apiClub <- client.get[ApiClub](ApiClub.getUrl(slug))
          club = fromApi(apiClub, slug)
          _ <- upsertResolvingSlugConflict(club, client)
        } yield Option(apiClub.clubId)).catchAll(_ => ZIO.none)
    }

  /** Same `latest_match_at` semantics as [[upsert]]: not touched on update — managed by ClubDataApp. */
  def upsertBatch(clubs: Iterable[Club]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(clubs) { club =>
        sql"""INSERT INTO club (club_id, created, slug, name, members_count, latest_match_at)
              VALUES (${club.clubId}, ${club.created}, ${club.slug}, ${club.name}, ${club.membersCount}, ${club.latestMatchAt})
              ON CONFLICT (club_id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                members_count = EXCLUDED.members_count""".update
      }
    }
}
