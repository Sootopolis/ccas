package ccas.analysis.tables

import ccas.api.misc.subtypes.{ClubId, ClubUrlName}
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.SqlRepoUtils
import ccas.utils.sql.DbCodecs.given
import com.augustnagro.magnum.*
import zio.ZIO

import java.sql.SQLException
import java.time.Instant

case class Club(
  clubId : ClubId,
  created: Instant,
  urlName: ClubUrlName,
) derives DbCodec

object Club extends SqlRepoUtils {
  override protected type Repo = ClubRepository

  override protected def makeRepo(xa: Transactor): Repo = ClubRepository(xa)

  // Raw SQL — composable in transact() blocks (writes only)
  def upsertSql(club: Club)(using DbCon): Unit = {
    sql"""INSERT INTO club (club_id, created, url_name) VALUES (${club.clubId}, ${club.created}, ${club.urlName})
          ON CONFLICT (club_id) DO UPDATE SET url_name = EXCLUDED.url_name""".update.run()
    ()
  }

  def upsertBatchSql(clubs: Iterable[Club])(using DbCon): Unit = {
    batchUpdate(clubs) { club =>
      sql"""INSERT INTO club (club_id, created, url_name) VALUES (${club.clubId}, ${club.created}, ${club.urlName})
            ON CONFLICT (club_id) DO UPDATE SET url_name = EXCLUDED.url_name""".update
    }
    ()
  }

  // ZIO API
  def selectAll: RepoTask[List[Club]] = repoService(_.selectAll)
  def selectId(clubId: ClubId): RepoTask[Option[Club]] = repoService(_.selectId(clubId))
  def upsert(club: Club): RepoTask[Unit] = repoService(_.upsert(club))
  def upsertBatch(clubs: Iterable[Club]): RepoTask[Unit] = repoService(_.upsertBatch(clubs))

  case class ClubRepository(xa: Transactor) {
    def selectAll: SqlTask[List[Club]] =
      ZIO.attempt { connect(xa)(sql"SELECT club_id, created, url_name FROM club".query[Club].run().toList) }
        .refineToOrDie[SQLException]

    def selectId(clubId: ClubId): SqlTask[Option[Club]] =
      ZIO.attempt { connect(xa)(sql"SELECT club_id, created, url_name FROM club WHERE club_id = $clubId".query[Club].run().headOption) }
        .refineToOrDie[SQLException]

    def upsert(club: Club): SqlTask[Unit] =
      ZIO.attempt { connect(xa)(upsertSql(club)) }
        .refineToOrDie[SQLException]

    def upsertBatch(clubs: Iterable[Club]): SqlTask[Unit] =
      ZIO.attempt { transact(xa)(upsertBatchSql(clubs)) }
        .refineToOrDie[SQLException]
  }
}
