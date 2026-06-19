package ccas.server.scheduler

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubId
import ccas.server.jobs.JobKind
import ccas.server.jobs.JobKind.given
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class JobSchedule(
  @Id id: Long,
  kind: JobKind,
  clubId: Option[ClubId],
  params: Option[String],
  intervalHours: Short,
  enabled: Boolean,
  lastRunAt: Option[Instant]
) derives DbCodec

object JobSchedule {

  private val columns = SqlLiteral("id, kind, club_id, params, interval_hours, enabled, last_run_at")

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS job_schedule (
              id              BIGSERIAL PRIMARY KEY,
              kind            TEXT NOT NULL,
              club_id         BIGINT REFERENCES club (club_id) ON DELETE RESTRICT,
              params          TEXT,
              interval_hours  SMALLINT NOT NULL,
              enabled         BOOLEAN NOT NULL,
              last_run_at     TIMESTAMPTZ,
              UNIQUE (kind, club_id)
            )""".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[JobSchedule]] =
    connectZIO {
      sql"SELECT $columns FROM job_schedule ORDER BY id".query[JobSchedule].run().toList
    }

  def selectEnabled: ZIO[PostgresClient, SQLException, List[JobSchedule]] =
    connectZIO {
      sql"SELECT $columns FROM job_schedule WHERE enabled = TRUE".query[JobSchedule].run().toList
    }

  def selectId(id: Long): ZIO[PostgresClient, SQLException, Option[JobSchedule]] =
    connectZIO {
      sql"SELECT $columns FROM job_schedule WHERE id = $id".query[JobSchedule].run().headOption
    }

  /** Idempotently seeds a global (all-clubs, `club_id IS NULL`) maintenance schedule. Uses a NULL-safe
    * `WHERE NOT EXISTS` guard rather than `ON CONFLICT (kind, club_id) DO NOTHING`: the table's
    * `UNIQUE (kind, club_id)` treats NULL `club_id` as distinct, so ON CONFLICT would re-insert a duplicate
    * global row on every boot. A row already present (incl. one hand-disabled by the operator) is left untouched.
    * Returns the rows inserted (1 on first seed, 0 thereafter).
    */
  def seedGlobalIfAbsent(seed: ScheduleSeed): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO job_schedule (kind, club_id, params, interval_hours, enabled, last_run_at)
            SELECT ${seed.kind}, NULL, NULL, ${seed.intervalHours}, ${seed.enabled}, NULL
            WHERE NOT EXISTS (
              SELECT 1 FROM job_schedule WHERE kind = ${seed.kind} AND club_id IS NULL
            )""".update.run()
    }

  def insert(schedule: JobSchedule): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO job_schedule (kind, club_id, params, interval_hours, enabled, last_run_at)
            VALUES (${schedule.kind}, ${schedule.clubId}, ${schedule.params},
                    ${schedule.intervalHours}, ${schedule.enabled}, ${schedule.lastRunAt})
            RETURNING id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def updateLastRunAt(id: Long, at: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"UPDATE job_schedule SET last_run_at = $at WHERE id = $id".update.run()
    }

  def update(
    id: Long,
    intervalHours: Option[Short],
    enabled: Option[Boolean],
    params: Option[Option[String]]
  ): ZIO[PostgresClient, SQLException, Int] =
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

  def delete(id: Long): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM job_schedule WHERE id = $id".update.run()
    }
}
