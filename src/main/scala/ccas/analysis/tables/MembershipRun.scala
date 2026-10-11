package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.RunTrigger.given
import ccas.analysis.tables.subtypes.MembershipRunId
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

final case class MembershipRun(
  runId: MembershipRunId,
  clubId: ClubId,
  trigger: RunTrigger,
  startedAt: Instant,
  completedAt: Option[Instant],
  @SqlName("job_run_id") jobRunIdOption: Option[JobRunId]
) derives DbCodec

object MembershipRun {
  private val selectCols = SqlLiteral("run_id, club_id, trigger, started_at, completed_at, job_run_id")

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      sql"""CREATE TABLE IF NOT EXISTS membership_run (
              run_id       BIGSERIAL PRIMARY KEY,
              club_id      BIGINT NOT NULL,
              trigger      TEXT NOT NULL,
              started_at   TIMESTAMPTZ NOT NULL,
              completed_at TIMESTAMPTZ,
              job_run_id   TEXT,
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_membership_run_club_started
            ON membership_run (club_id, started_at DESC)""".update.run()
    }

  /** The latest run that persisted what it saw, whatever started it: a run that failed persisted nothing. */
  def selectLatestCompleted(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[MembershipRun]] =
    connectZIO {
      sql"""SELECT $selectCols FROM membership_run WHERE club_id = $clubId AND completed_at IS NOT NULL
            ORDER BY started_at DESC LIMIT 1""".query[MembershipRun].run().headOption
    }

  /** The latest completed run that reported its changes: one another job ran as a step of its own does not, so the
    * next report still shows what it saw.
    */
  def selectLatestReported(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[MembershipRun]] =
    connectZIO {
      sql"""SELECT $selectCols FROM membership_run
            WHERE club_id = $clubId AND completed_at IS NOT NULL AND trigger <> ${RunTrigger.Dependency: RunTrigger}
            ORDER BY started_at DESC LIMIT 1"""
        .query[MembershipRun].run().headOption
    }

  def insert(
    clubId: ClubId,
    trigger: RunTrigger,
    startedAt: Instant,
    jobRunIdOption: Option[JobRunId]
  ): ZIO[PostgresClient, SQLException, MembershipRunId] =
    connectZIO {
      sql"""INSERT INTO membership_run (club_id, trigger, started_at, job_run_id)
            VALUES ($clubId, $trigger, $startedAt, $jobRunIdOption)
            RETURNING run_id""".query[MembershipRunId].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def complete(runId: MembershipRunId, completedAt: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE membership_run SET completed_at = $completedAt
            WHERE run_id = $runId""".update.run()
    }
}
