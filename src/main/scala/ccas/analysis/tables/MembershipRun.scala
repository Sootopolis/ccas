package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.RunTrigger.given
import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class MembershipRun(
  runId: Long,
  clubId: ClubId,
  trigger: RunTrigger,
  startedAt: Instant,
  completedAt: Option[Instant]
) derives DbCodec

object MembershipRun {
  private val selectCols = SqlLiteral("run_id, club_id, trigger, started_at, completed_at")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS membership_run (
              run_id       BIGSERIAL PRIMARY KEY,
              club_id      BIGINT NOT NULL,
              trigger      TEXT NOT NULL,
              started_at   TIMESTAMPTZ NOT NULL,
              completed_at TIMESTAMPTZ,
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_membership_run_club_id ON membership_run(club_id)""".update.run()
    }

  def insert(clubId: ClubId, trigger: RunTrigger, startedAt: Instant): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO membership_run (club_id, trigger, started_at)
            VALUES ($clubId, $trigger, $startedAt)
            RETURNING run_id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def complete(runId: Long, completedAt: Instant): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE membership_run SET completed_at = $completedAt
            WHERE run_id = $runId""".update.run()
    }

  def selectLatest(clubId: ClubId): ZIO[Transactor, SQLException, Option[MembershipRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM membership_run WHERE club_id = $clubId ORDER BY started_at DESC"
        .query[MembershipRun].run().headOption
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM membership_run".update.run()
    }
}
