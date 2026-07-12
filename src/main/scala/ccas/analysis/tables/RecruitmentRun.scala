package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.RunTrigger.given
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class RecruitmentRun(
  runId: RecruitmentRunId,
  clubId: ClubId,
  criteriaId: Long,
  trigger: RunTrigger,
  startedAt: Instant,
  completedAt: Option[Instant],
  candidatesFound: Int,
  jobRunId: Option[JobRunId]
) derives DbCodec

object RecruitmentRun {
  private val selectCols = SqlLiteral(
    "run_id, club_id, criteria_id, trigger, started_at, completed_at, candidates_found, job_run_id"
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_run (
              run_id            BIGSERIAL PRIMARY KEY,
              club_id           BIGINT NOT NULL,
              criteria_id       BIGINT NOT NULL,
              trigger           TEXT NOT NULL,
              started_at        TIMESTAMPTZ NOT NULL,
              completed_at      TIMESTAMPTZ,
              candidates_found  INT NOT NULL,
              job_run_id        TEXT,
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT,
              FOREIGN KEY (criteria_id) REFERENCES recruitment_criteria (criteria_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_recruitment_run_club_started
            ON recruitment_run (club_id, started_at DESC)""".update.run()
    }

  def selectId(runId: RecruitmentRunId): ZIO[PostgresClient, SQLException, Option[RecruitmentRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_run WHERE run_id = $runId".query[RecruitmentRun].run().headOption
    }

  def selectLatest(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[RecruitmentRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_run WHERE club_id = $clubId ORDER BY started_at DESC LIMIT 1"
        .query[RecruitmentRun].run().headOption
    }

  // One recruit job inserts exactly one run, so at most one row matches; ORDER BY + LIMIT 1 is defensive belt-and-braces.
  def selectByJobRunId(jobRunId: JobRunId): ZIO[PostgresClient, SQLException, Option[RecruitmentRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_run WHERE job_run_id = $jobRunId ORDER BY started_at DESC LIMIT 1"
        .query[RecruitmentRun].run().headOption
    }

  def sumCandidatesFoundToday(clubId: ClubId, alias: String): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""SELECT COALESCE(SUM(candidates_found), 0) FROM recruitment_run
            WHERE club_id = $clubId AND criteria_id IN (
              SELECT criteria_id FROM recruitment_alias WHERE club_id = $clubId AND alias = $alias
            )
              AND completed_at IS NOT NULL
              AND started_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
              AND started_at < date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 day'"""
        .query[Int].run().headOption
    }.someOrFail(new SQLException("COALESCE query produced no rows"))

  def insert(
    clubId: ClubId,
    criteriaId: Long,
    trigger: RunTrigger,
    startedAt: Instant,
    jobRunId: Option[JobRunId]
  ): ZIO[PostgresClient, SQLException, RecruitmentRunId] =
    connectZIO {
      sql"""INSERT INTO recruitment_run (club_id, criteria_id, trigger, started_at, candidates_found, job_run_id)
            VALUES ($clubId, $criteriaId, $trigger, $startedAt, 0, $jobRunId)
            RETURNING run_id""".query[RecruitmentRunId].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def update(item: RecruitmentRun): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE recruitment_run SET completed_at = ${item.completedAt}, candidates_found = ${item.candidatesFound}
            WHERE run_id = ${item.runId}""".update.run()
    }

  // Set candidates_found without touching completed_at — used when a deferred-confirm run is confirmed after the fact
  // (the scout left it at 0; confirming flips its Deferred candidates to Invited and records how many). Rows affected.
  def setCandidatesFound(runId: RecruitmentRunId, count: Int): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"UPDATE recruitment_run SET candidates_found = $count WHERE run_id = $runId".update.run()
    }
}
