package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant
import java.time.temporal.ChronoUnit

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.RunTrigger.given
import ccas.analysis.tables.subtypes.{JobRunId, RecruitmentRunId}
import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

final case class RecruitmentRun(
  runId: RecruitmentRunId,
  clubId: ClubId,
  criteriaId: Long,
  trigger: RunTrigger,
  startedAt: Instant,
  completedAt: Option[Instant],
  candidatesFound: Int,
  // The run's effective target (invite budget). Read by `selectDeferredByRun` / `confirmDeferredByRun` to cap the
  // interactive confirm at target, leaving any chunk-overshoot excess Deferred to carry forward. NULL only for legacy
  // rows created before this column existed (treated as "no cap").
  target: Option[Int],
  @SqlName("job_run_id") jobRunIdOption: Option[JobRunId]
) derives DbCodec

object RecruitmentRun {

  /** The UTC day an instant falls on, as the bounds a run's `started_at` is compared with. A whole day is 24 hours
    * here, where an interval added in SQL would follow the session's time zone across a clock change.
    */
  final case class UtcDay(start: Instant, end: Instant)

  object UtcDay {
    def of(instant: Instant): UtcDay = {
      val start = instant.truncatedTo(ChronoUnit.DAYS)
      UtcDay(start, start.plus(1, ChronoUnit.DAYS))
    }
  }

  private val selectCols = SqlLiteral(
    "run_id, club_id, criteria_id, trigger, started_at, completed_at, candidates_found, target, job_run_id"
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_run (
              run_id            BIGSERIAL PRIMARY KEY,
              club_id           BIGINT NOT NULL,
              criteria_id       BIGINT NOT NULL,
              trigger           TEXT NOT NULL,
              started_at        TIMESTAMPTZ NOT NULL,
              completed_at      TIMESTAMPTZ,
              candidates_found  INT NOT NULL,
              target            INT,
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
  def selectByJobRunId(jobRunIdOption: JobRunId): ZIO[PostgresClient, SQLException, Option[RecruitmentRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_run WHERE job_run_id = $jobRunIdOption ORDER BY started_at DESC LIMIT 1"
        .query[RecruitmentRun].run().headOption
    }

  /** Candidates found by `alias`'s completed runs that started on the UTC day of `dayOf`. */
  def sumCandidatesFoundToday(clubId: ClubId, alias: String, dayOf: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      val day = UtcDay.of(dayOf)
      sql"""SELECT COALESCE(SUM(candidates_found), 0) FROM recruitment_run
            WHERE club_id = $clubId AND criteria_id IN (
              SELECT criteria_id FROM recruitment_alias WHERE club_id = $clubId AND alias = $alias
            )
              AND completed_at IS NOT NULL
              AND started_at >= ${day.start} AND started_at < ${day.end}"""
        .query[Int].run().headOption
    }.someOrFail(new SQLException("COALESCE query produced no rows"))

  def insert(
    clubId: ClubId,
    criteriaId: Long,
    trigger: RunTrigger,
    startedAt: Instant,
    target: Option[Int],
    jobRunIdOption: Option[JobRunId]
  ): ZIO[PostgresClient, SQLException, RecruitmentRunId] =
    connectZIO {
      sql"""INSERT INTO recruitment_run (club_id, criteria_id, trigger, started_at, candidates_found, target, job_run_id)
            VALUES ($clubId, $criteriaId, $trigger, $startedAt, 0, $target, $jobRunIdOption)
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
