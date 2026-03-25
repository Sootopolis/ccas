package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class Club(@Id clubId: ClubId, created: Instant, slug: ClubSlug) derives DbCodec

object Club {
  private val repo = ImmutableRepo[Club, ClubId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club (
              club_id  BIGINT PRIMARY KEY,
              created  TIMESTAMPTZ NOT NULL,
              slug VARCHAR NOT NULL
            )""".update.run()
    }

  def selectAll: ZIO[Transactor, SQLException, List[Club]] =
    connectZIO(repo.findAll.toList)

  def selectId(clubId: ClubId): ZIO[Transactor, SQLException, Option[Club]] =
    connectZIO(repo.findById(clubId))

  def selectBySlug(slug: ClubSlug): ZIO[Transactor, SQLException, Option[Club]] =
    connectZIO {
      sql"SELECT club_id, created, slug FROM club WHERE slug = $slug"
        .query[Club].run().headOption
    }

  def upsert(club: Club): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club (club_id, created, slug) VALUES (${club.clubId}, ${club.created}, ${club.slug})
            ON CONFLICT (club_id) DO UPDATE SET slug = EXCLUDED.slug""".update.run()
    }

  def upsertBatch(clubs: Iterable[Club]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(clubs) { club =>
        sql"""INSERT INTO club (club_id, created, slug) VALUES (${club.clubId}, ${club.created}, ${club.slug})
              ON CONFLICT (club_id) DO UPDATE SET slug = EXCLUDED.slug""".update
      }
    }
}
