package ccas.server.scheduler

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubUrlName
import ccas.server.jobs.JobKind
import ccas.server.jobs.JobKind.given
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class JobSchedule(
    @Id id: Long,
    kind: JobKind,
    clubUrlName: Option[ClubUrlName],
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
              club_url_name   TEXT,
              params          TEXT,
              interval_hours  INT NOT NULL,
              enabled         BOOLEAN NOT NULL DEFAULT TRUE,
              last_run_at     TIMESTAMPTZ,
              UNIQUE (kind, club_url_name)
            )""".update.run()
    }

  private val columns = SqlLiteral("id, kind, club_url_name, params, interval_hours, enabled, last_run_at")

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
      sql"""INSERT INTO job_schedule (kind, club_url_name, params, interval_hours, enabled, last_run_at)
            VALUES (${schedule.kind}, ${schedule.clubUrlName}, ${schedule.params},
                    ${schedule.intervalHours}, ${schedule.enabled}, ${schedule.lastRunAt})
            RETURNING id""".query[Long].run().head
    }

  def updateLastRunAt(id: Long, at: Instant): ZIO[Transactor, SQLException, Unit] =
    connectZIO {
      (sql"UPDATE job_schedule SET last_run_at = $at WHERE id = $id".update.run()): Unit
    }

  def update(id: Long, intervalHours: Option[Int], enabled: Option[Boolean], params: Option[Option[String]]): ZIO[Transactor, SQLException, Unit] =
    connectZIO {
      val _ = (intervalHours, enabled, params) match
        case (Some(ih), Some(en), Some(p)) =>
          sql"UPDATE job_schedule SET interval_hours = $ih, enabled = $en, params = $p WHERE id = $id".update.run()
        case (Some(ih), Some(en), None) =>
          sql"UPDATE job_schedule SET interval_hours = $ih, enabled = $en WHERE id = $id".update.run()
        case (Some(ih), None, Some(p)) =>
          sql"UPDATE job_schedule SET interval_hours = $ih, params = $p WHERE id = $id".update.run()
        case (Some(ih), None, None) =>
          sql"UPDATE job_schedule SET interval_hours = $ih WHERE id = $id".update.run()
        case (None, Some(en), Some(p)) =>
          sql"UPDATE job_schedule SET enabled = $en, params = $p WHERE id = $id".update.run()
        case (None, Some(en), None) =>
          sql"UPDATE job_schedule SET enabled = $en WHERE id = $id".update.run()
        case (None, None, Some(p)) =>
          sql"UPDATE job_schedule SET params = $p WHERE id = $id".update.run()
        case (None, None, None) => 0
    }

  def delete(id: Long): ZIO[Transactor, SQLException, Unit] =
    connectZIO {
      val _ = sql"DELETE FROM job_schedule WHERE id = $id".update.run()
    }
}
