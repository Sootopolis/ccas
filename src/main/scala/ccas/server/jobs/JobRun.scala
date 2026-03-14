package ccas.server.jobs

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubUrlName
import ccas.server.jobs.JobKind.given
import ccas.server.jobs.JobRunStatus.given
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class JobRun(
    @Id id: JobRunId,
    kind: JobKind,
    status: JobRunStatus,
    clubUrlName: Option[ClubUrlName],
    params: Option[String],
    startedAt: Instant,
    completedAt: Option[Instant],
    error: Option[String]
) derives DbCodec

object JobRun {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS job_run (
              id             TEXT PRIMARY KEY,
              kind           TEXT NOT NULL,
              status         TEXT NOT NULL,
              club_url_name  TEXT,
              params         TEXT,
              started_at     TIMESTAMPTZ NOT NULL,
              completed_at   TIMESTAMPTZ,
              error          TEXT
            )""".update.run()
    }

  def insert(jobRun: JobRun): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO job_run (id, kind, status, club_url_name, params, started_at, completed_at, error)
             VALUES (${jobRun.id}, ${jobRun.kind}, ${jobRun.status}, ${jobRun.clubUrlName},
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

  def selectId(id: JobRunId): ZIO[Transactor, SQLException, Option[JobRun]] =
    connectZIO {
      sql"SELECT id, kind, status, club_url_name, params, started_at, completed_at, error FROM job_run WHERE id = $id"
        .query[JobRun].run().headOption
    }

  def selectRunning(kind: JobKind, clubUrlName: Option[ClubUrlName]): ZIO[Transactor, SQLException, Option[JobRun]] =
    connectZIO {
      clubUrlName match
        case Some(name) =>
          sql"""SELECT id, kind, status, club_url_name, params, started_at, completed_at, error
                FROM job_run WHERE kind = $kind AND club_url_name = $name AND status = 'Running'"""
            .query[JobRun].run().headOption
        case None =>
          sql"""SELECT id, kind, status, club_url_name, params, started_at, completed_at, error
                FROM job_run WHERE kind = $kind AND club_url_name IS NULL AND status = 'Running'"""
            .query[JobRun].run().headOption
    }

  def selectRecent(limit: Int): ZIO[Transactor, SQLException, List[JobRun]] =
    connectZIO {
      sql"""SELECT id, kind, status, club_url_name, params, started_at, completed_at, error
            FROM job_run ORDER BY started_at DESC LIMIT $limit"""
        .query[JobRun].run().toList
    }

  def markOrphansAsFailed: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE job_run SET status = 'Failed', completed_at = NOW(), error = 'Service restarted'
            WHERE status = 'Running'""".update.run()
    }
}
