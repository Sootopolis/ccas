package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class MembershipRun(runId: Long, clubId: ClubId, ranAt: Instant) derives DbCodec

object MembershipRun {
  private val selectCols = SqlLiteral("run_id, club_id, ran_at")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS membership_run (
              run_id   BIGSERIAL PRIMARY KEY,
              club_id  BIGINT NOT NULL,
              ran_at   TIMESTAMPTZ NOT NULL,
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_membership_run_club_id ON membership_run(club_id)""".update.run()
    }

  def insert(clubId: ClubId, ranAt: Instant): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO membership_run (club_id, ran_at)
            VALUES ($clubId, $ranAt)
            RETURNING run_id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def selectLatest(clubId: ClubId): ZIO[Transactor, SQLException, Option[MembershipRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM membership_run WHERE club_id = $clubId ORDER BY ran_at DESC"
        .query[MembershipRun].run().headOption
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM membership_run".update.run()
    }
}
