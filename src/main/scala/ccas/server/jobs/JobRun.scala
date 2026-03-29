package ccas.server.jobs

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.RunTrigger
import ccas.analysis.tables.RunTrigger.given
import ccas.api.misc.subtypes.ClubId
import ccas.server.jobs.JobKind.given
import ccas.server.jobs.JobRunStatus.given
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class JobRun(
  @Id id: JobRunId,
  kind: JobKind,
  clubId: Option[ClubId],
  trigger: RunTrigger,
  status: JobRunStatus,
  params: Option[String],
  startedAt: Instant,
  completedAt: Option[Instant],
  error: Option[String]
) derives DbCodec

object JobRun {

  private val selectCols = SqlLiteral("id, kind, club_id, trigger, status, params, started_at, completed_at, error")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS job_run (
              id             TEXT PRIMARY KEY,
              kind           TEXT NOT NULL,
              club_id        BIGINT REFERENCES club (club_id),
              trigger        TEXT NOT NULL,
              status         TEXT NOT NULL,
              params         TEXT,
              started_at     TIMESTAMPTZ NOT NULL,
              completed_at   TIMESTAMPTZ,
              error          TEXT
            )""".update.run()
    }

  def insert(jobRun: JobRun): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO job_run (id, kind, club_id, trigger, status, params, started_at, completed_at, error)
             VALUES (${jobRun.id}, ${jobRun.kind}, ${jobRun.clubId}, ${jobRun.trigger}, ${jobRun.status},
                     ${jobRun.params}, ${jobRun.startedAt}, ${jobRun.completedAt}, ${jobRun.error})
          """.update.run()
    }

  def update(jobRun: JobRun): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE job_run
             SET status = ${jobRun.status}, completed_at = ${jobRun.completedAt}, error = ${jobRun.error}
             WHERE id = ${jobRun.id}
          """.update.run()
    }

  def updateStatus(
    id: JobRunId,
    status: JobRunStatus,
    completedAt: Option[Instant],
    error: Option[String]
  ): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE job_run SET status = $status, completed_at = $completedAt, error = $error
             WHERE id = $id""".update.run()
    }

  def selectId(id: JobRunId): ZIO[Transactor, SQLException, Option[JobRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM job_run WHERE id = $id"
        .query[JobRun].run().headOption
    }

  def selectRunning(kind: JobKind, clubId: Option[ClubId]): ZIO[Transactor, SQLException, Option[JobRun]] =
    connectZIO {
      val running = JobRunStatus.Running
      clubId match {
        case Some(cid) =>
          sql"SELECT $selectCols FROM job_run WHERE kind = $kind AND club_id = $cid AND status = $running"
            .query[JobRun].run().headOption
        case None =>
          sql"SELECT $selectCols FROM job_run WHERE kind = $kind AND club_id IS NULL AND status = $running"
            .query[JobRun].run().headOption
      }
    }

  def selectRecent(limit: Int): ZIO[Transactor, SQLException, List[JobRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM job_run ORDER BY started_at DESC LIMIT $limit"
        .query[JobRun].run().toList
    }

  def markOrphansAsFailed: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      val failed  = JobRunStatus.Failed
      val running = JobRunStatus.Running
      sql"""UPDATE job_run SET status = $failed, completed_at = NOW(), error = 'Service restarted'
            WHERE status = $running""".update.run()
    }
}
