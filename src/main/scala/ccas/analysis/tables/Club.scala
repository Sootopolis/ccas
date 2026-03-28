package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
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
              name     VARCHAR NOT NULL DEFAULT ''
            )""".update.run()
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

  /** Resolves a club slug to its ID, fetching from the Chess.com API and persisting if not already in the database. */
  def resolveOrFetch(client: ChessComClient, slug: ClubSlug): ZIO[Transactor, SQLException, Option[ClubId]] =
    selectBySlug(slug).flatMap {
      case Some(club) => ZIO.some(club.clubId)
      case None =>
        (for {
          apiClub <- client.get[ApiClub](ApiClub.getUrl(slug))
          club = Club(apiClub.clubId, Instant.ofEpochSecond(apiClub.created), slug, apiClub.name)
          _ <- upsert(club)
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
