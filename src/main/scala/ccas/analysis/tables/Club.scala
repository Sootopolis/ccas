package ccas.analysis.tables

import ccas.api.misc.subtypes.{ClubId, ClubUrlName}
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}
import ccas.utils.sql.DbCodecs.given
import com.augustnagro.magnum.*
import zio.ZIO

import java.sql.SQLException
import java.time.Instant

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class Club(
  @Id clubId: ClubId,
  created   : Instant,
  urlName   : ClubUrlName,
) derives DbCodec

object Club {
  private val repo = ImmutableRepo[Club, ClubId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club (
              club_id  BIGINT PRIMARY KEY,
              created  TIMESTAMPTZ NOT NULL,
              url_name VARCHAR NOT NULL
            )""".update.run()
    }

  def selectAll: ZIO[Transactor, SQLException, List[Club]] =
    connectZIO(repo.findAll.toList)

  def selectId(clubId: ClubId): ZIO[Transactor, SQLException, Option[Club]] =
    connectZIO(repo.findById(clubId))

  def upsert(club: Club): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club (club_id, created, url_name) VALUES (${club.clubId}, ${club.created}, ${club.urlName})
            ON CONFLICT (club_id) DO UPDATE SET url_name = EXCLUDED.url_name""".update.run()
    }

  def upsertBatch(clubs: Iterable[Club]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(clubs) { club =>
        sql"""INSERT INTO club (club_id, created, url_name) VALUES (${club.clubId}, ${club.created}, ${club.urlName})
              ON CONFLICT (club_id) DO UPDATE SET url_name = EXCLUDED.url_name""".update
      }
    }
}
