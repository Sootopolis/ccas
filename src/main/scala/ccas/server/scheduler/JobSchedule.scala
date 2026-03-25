package ccas.server.scheduler

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubSlug
import ccas.server.jobs.JobKind
import ccas.server.jobs.JobKind.given
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class JobSchedule(
  @Id id: Long,
  kind: JobKind,
  clubSlug: Option[ClubSlug],
  params: Option[String],
  intervalHours: Int,
  enabled: Boolean,
  lastRunAt: Option[Instant]
) derives DbCodec

object JobSchedule {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS job_schedule (
              id              BIGSERIAL PRIMARY KEY,
              kind            TEXT NOT NULL,
              club_slug   TEXT,
              params          TEXT,
              interval_hours  INT NOT NULL,
              enabled         BOOLEAN NOT NULL DEFAULT TRUE,
              last_run_at     TIMESTAMPTZ,
              UNIQUE (kind, club_slug)
            )""".update.run()
    }

  private val columns = SqlLiteral("id, kind, club_slug, params, interval_hours, enabled, last_run_at")

  def selectAll: ZIO[Transactor, SQLException, List[JobSchedule]] =
    connectZIO {
      sql"SELECT $columns FROM job_schedule ORDER BY id".query[JobSchedule].run().toList
    }

  def selectEnabled: ZIO[Transactor, SQLException, List[JobSchedule]] =
    connectZIO {
      sql"SELECT $columns FROM job_schedule WHERE enabled = TRUE".query[JobSchedule].run().toList
    }

  def selectId(id: Long): ZIO[Transactor, SQLException, Option[JobSchedule]] =
    connectZIO {
      sql"SELECT $columns FROM job_schedule WHERE id = $id".query[JobSchedule].run().headOption
    }

  def insert(schedule: JobSchedule): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO job_schedule (kind, club_slug, params, interval_hours, enabled, last_run_at)
            VALUES (${schedule.kind}, ${schedule.clubSlug}, ${schedule.params},
                    ${schedule.intervalHours}, ${schedule.enabled}, ${schedule.lastRunAt})
            RETURNING id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def updateLastRunAt(id: Long, at: Instant): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"UPDATE job_schedule SET last_run_at = $at WHERE id = $id".update.run()
    }

  def update(
    id: Long,
    intervalHours: Option[Int],
    enabled: Option[Boolean],
    params: Option[Option[String]]
  ): ZIO[Transactor, SQLException, Int] =
    if (intervalHours.isEmpty && enabled.isEmpty && params.isEmpty) ZIO.succeed(0)
    else {
      val hasParams   = params.isDefined
      val paramsValue = params.flatten
      connectZIO {
        sql"""UPDATE job_schedule SET
                interval_hours = COALESCE($intervalHours, interval_hours),
                enabled = COALESCE($enabled, enabled),
                params = CASE WHEN $hasParams THEN $paramsValue ELSE params END
              WHERE id = $id""".update.run()
      }
    }

  def delete(id: Long): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM job_schedule WHERE id = $id".update.run()
    }
}
