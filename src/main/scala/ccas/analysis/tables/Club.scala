package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.analysis.apps.ref.RefHelpers
import ccas.utils.client.ChessComClient
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class Club(@Id clubId: ClubId, created: Instant, slug: ClubSlug, name: String) derives DbCodec

object Club {
  private val repo = ImmutableRepo[Club, ClubId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club (
              club_id  BIGINT PRIMARY KEY,
              created  TIMESTAMPTZ NOT NULL,
              slug     VARCHAR NOT NULL,
              name     VARCHAR NOT NULL
            )""".update.run()
    } *> connectZIO {
      sql"CREATE UNIQUE INDEX IF NOT EXISTS club_slug_key ON club (slug)".update.run()
    }

  def selectAll: ZIO[Transactor, SQLException, List[Club]] =
    connectZIO(repo.findAll.toList)

  def selectId(clubId: ClubId): ZIO[Transactor, SQLException, Option[Club]] =
    connectZIO(repo.findById(clubId))

  def selectBySlug(slug: ClubSlug): ZIO[Transactor, SQLException, Option[Club]] =
    connectZIO {
      sql"SELECT club_id, created, slug, name FROM club WHERE slug = $slug"
        .query[Club].run().headOption
    }

  def upsert(club: Club): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club (club_id, created, slug, name) VALUES (${club.clubId}, ${club.created}, ${club.slug}, ${club.name})
            ON CONFLICT (club_id) DO UPDATE SET slug = EXCLUDED.slug, name = EXCLUDED.name""".update.run()
    }

  /** Upsert that handles slug conflicts by resolving the stale club's current slug via match ref.
    *
    * When another club already holds the target slug in the database, this method looks up one of
    * the stale club's matches and fetches the team URL from the Chess.com API to discover its
    * current slug. Falls back to a placeholder if the stale club has no matches.
    */
  def upsertResolvingSlugConflict(club: Club, client: ChessComClient): ZIO[Transactor, Throwable, Int] =
    for {
      existing <- selectBySlug(club.slug)
      _ <- existing match {
        case Some(stale) if stale.clubId != club.clubId => resolveStaleSlug(stale, client)
        case _                                          => ZIO.unit
      }
      result <- upsert(club)
    } yield result

  private def resolveStaleSlug(stale: Club, client: ChessComClient): ZIO[Transactor, Throwable, Unit] =
    ClubMatch.selectClubMatchRef(stale.clubId).flatMap {
      case Some(ref) =>
        RefHelpers.fetchTeamMatchTeams(client, ref.matchId, ref.isLive).flatMap { teams =>
          val team = if (ref.isTeam1) { teams.team1 } else { teams.team2 }
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

  /** Resolves a club slug to its ID, fetching from the Chess.com API and persisting if not already in the database. */
  def resolveOrFetch(client: ChessComClient, slug: ClubSlug): ZIO[Transactor, SQLException, Option[ClubId]] =
    selectBySlug(slug).flatMap {
      case Some(club) => ZIO.some(club.clubId)
      case None =>
        (for {
          apiClub <- client.get[ApiClub](ApiClub.getUrl(slug))
          club = Club(apiClub.clubId, Instant.ofEpochSecond(apiClub.created), slug, apiClub.name)
          _ <- upsertResolvingSlugConflict(club, client)
        } yield Option(apiClub.clubId)).catchAll(_ => ZIO.none)
    }

  def upsertBatch(clubs: Iterable[Club]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(clubs) { club =>
        sql"""INSERT INTO club (club_id, created, slug, name) VALUES (${club.clubId}, ${club.created}, ${club.slug}, ${club.name})
              ON CONFLICT (club_id) DO UPDATE SET slug = EXCLUDED.slug, name = EXCLUDED.name""".update
      }
    }
}
