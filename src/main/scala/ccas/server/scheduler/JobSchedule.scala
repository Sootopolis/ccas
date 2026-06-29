package ccas.server.scheduler

import java.sql.SQLException
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneId}

import com.augustnagro.magnum.*
import cron4s.lib.javatime.*
import cron4s.syntax.all.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubId
import ccas.server.jobs.JobKind
import ccas.server.jobs.JobKind.given
import ccas.server.scheduler.MisfirePolicy.given
import ccas.server.scheduler.TriggerType.given
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

/** A scheduled job. The trigger is a tagged union over flat columns (see [[trigger]]):
  *   - `Interval`: `interval_hours` set, cron columns NULL — fire when `interval_hours` have elapsed since
  *     `last_run_at`.
  *   - `Cron`: `cron_expr` (normalized 6-field) + `timezone` (IANA) + `misfire_policy` set, `interval_hours`
  *     NULL — fire at wall-clock boundaries.
  *
  * A DB CHECK (`job_schedule_trigger_shape`) enforces that exactly one trigger's columns are populated; the
  * smart constructors [[interval]] / [[cron]] build rows that satisfy it.
  */
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class JobSchedule(
  @Id id: Long,
  kind: JobKind,
  clubId: Option[ClubId],
  params: Option[String],
  triggerType: TriggerType,
  intervalHours: Option[Short],
  cronExpr: Option[String],
  timezone: Option[String],
  misfirePolicy: Option[MisfirePolicy],
  enabled: Boolean,
  lastRunAt: Option[Instant]
) derives DbCodec {

  /** Decode the flat columns into a typed trigger. Throws on a row that violates the column shape (a
    * hand-edited DB) — every write path validates first, and the scheduler wraps this call.
    */
  def trigger: ScheduleTrigger =
    triggerType match {
      case TriggerType.Interval =>
        ScheduleTrigger.Interval(
          intervalHours.getOrElse(throw new IllegalStateException(s"Interval schedule $id has no interval_hours"))
        )
      case TriggerType.Cron =>
        val raw  = cronExpr.getOrElse(throw new IllegalStateException(s"Cron schedule $id has no cron_expr"))
        val zone = ZoneId.of(timezone.getOrElse(throw new IllegalStateException(s"Cron schedule $id has no timezone")))
        val mp   = misfirePolicy.getOrElse(throw new IllegalStateException(s"Cron schedule $id has no misfire_policy"))
        val expr = cron4s.Cron.parse(raw).fold(
          err => throw new IllegalStateException(s"Cron schedule $id has an invalid cron_expr '$raw': $err"),
          identity
        )
        ScheduleTrigger.Cron(expr, zone, mp)
    }

  /** Whether this schedule should fire at `now`. Pure — drives [[JobScheduler]] and is TestClock-friendly.
    *   - Interval: at least `interval_hours` have elapsed since `last_run_at` (always due if never run).
    *   - Cron: the most recent fire boundary (`expr.prev(now)`) is after `last_run_at`, gated by misfire:
    *     `CatchUp` fires regardless of how late; `Skip` only fires a boundary that elapsed within `grace`
    *     (so a boundary missed during downtime is skipped, and the next on-time boundary resumes cleanly).
    */
  def isDue(now: Instant, grace: Duration): Boolean =
    trigger match {
      case ScheduleTrigger.Interval(hours) =>
        lastRunAt.forall(ts => ChronoUnit.HOURS.between(ts, now) >= hours)

      case ScheduleTrigger.Cron(expr, zone, misfire) =>
        expr.prev(now.atZone(zone)) match {
          case None => false
          case Some(prevZdt) =>
            val prevFire          = prevZdt.toInstant
            val boundaryAfterRun  = lastRunAt.forall(prevFire.isAfter)
            if (!boundaryAfterRun) { false }
            else {
              misfire match {
                case MisfirePolicy.CatchUp => true
                case MisfirePolicy.Skip    => Duration.between(prevFire, now).compareTo(grace) <= 0
              }
            }
        }
    }
}

object JobSchedule {

  private val columns = SqlLiteral(
    "id, kind, club_id, params, trigger_type, interval_hours, cron_expr, timezone, misfire_policy, enabled, last_run_at"
  )

  // Typed as `TriggerType` (not the singleton `TriggerType.Interval.type`) so Magnum's `sql` interpolation summons
  // `DbCodec[TriggerType]` and emits a bound `?` — interpolating the bare enum case emits nothing. Boot seeds are
  // always interval-triggered.
  private val intervalTrigger: TriggerType = TriggerType.Interval

  /** Smart constructor for an interval-triggered schedule (cron columns NULL). */
  def interval(
    id: Long,
    kind: JobKind,
    clubId: Option[ClubId],
    params: Option[String],
    intervalHours: Short,
    enabled: Boolean,
    lastRunAt: Option[Instant]
  ): JobSchedule =
    JobSchedule(id, kind, clubId, params, TriggerType.Interval, Some(intervalHours), None, None, None, enabled, lastRunAt)

  /** Smart constructor for a cron-triggered schedule (`interval_hours` NULL). `cronExpr` must be the
    * NORMALIZED 6-field string from [[ScheduleTrigger.validateCron]].
    */
  def cron(
    id: Long,
    kind: JobKind,
    clubId: Option[ClubId],
    params: Option[String],
    cronExpr: String,
    timezone: String,
    misfire: MisfirePolicy,
    enabled: Boolean,
    lastRunAt: Option[Instant]
  ): JobSchedule =
    JobSchedule(
      id,
      kind,
      clubId,
      params,
      TriggerType.Cron,
      None,
      Some(cronExpr),
      Some(timezone),
      Some(misfire),
      enabled,
      lastRunAt
    )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS job_schedule (
              id              BIGSERIAL PRIMARY KEY,
              kind            TEXT NOT NULL,
              club_id         BIGINT REFERENCES club (club_id) ON DELETE RESTRICT,
              params          TEXT,
              trigger_type    TEXT NOT NULL DEFAULT 'Interval',
              interval_hours  SMALLINT,
              cron_expr       TEXT,
              timezone        TEXT,
              misfire_policy  TEXT,
              enabled         BOOLEAN NOT NULL,
              last_run_at     TIMESTAMPTZ,
              UNIQUE (kind, club_id),
              CONSTRAINT job_schedule_trigger_shape CHECK (
                (trigger_type = 'Interval'
                   AND interval_hours IS NOT NULL
                   AND cron_expr IS NULL AND timezone IS NULL AND misfire_policy IS NULL)
                OR
                (trigger_type = 'Cron'
                   AND cron_expr IS NOT NULL AND timezone IS NOT NULL AND misfire_policy IS NOT NULL
                   AND interval_hours IS NULL)
              )
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
    * Boot seeds are always interval-triggered. Returns the rows inserted (1 on first seed, 0 thereafter).
    */
  def seedGlobalIfAbsent(seed: ScheduleSeed): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO job_schedule
              (kind, club_id, params, trigger_type, interval_hours, cron_expr, timezone, misfire_policy, enabled, last_run_at)
            SELECT ${seed.kind}, NULL, NULL, ${intervalTrigger}, ${seed.intervalHours}, NULL, NULL, NULL, ${seed.enabled}, NULL
            WHERE NOT EXISTS (
              SELECT 1 FROM job_schedule WHERE kind = ${seed.kind} AND club_id IS NULL
            )""".update.run()
    }

  /** Idempotently seeds a per-club (`club_id` non-NULL) schedule for one managed club (#102). Unlike
    * [[seedGlobalIfAbsent]], this relies on `ON CONFLICT (kind, club_id) DO NOTHING`: a non-NULL `club_id` is
    * deduped by the table's `UNIQUE (kind, club_id)`, so a re-seed conflicts on the existing row and inserts
    * nothing. (The global seeder cannot use ON CONFLICT because SQL treats NULL `club_id` as distinct, so every
    * boot would re-insert a duplicate global row.) A row already present — including one a user has edited or
    * disabled — is left untouched. Boot seeds are always interval-triggered. Returns rows inserted (1 on first
    * seed for this club+kind, 0 thereafter).
    */
  def seedPerClubIfAbsent(clubId: ClubId, seed: ScheduleSeed): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO job_schedule
              (kind, club_id, params, trigger_type, interval_hours, cron_expr, timezone, misfire_policy, enabled, last_run_at)
            VALUES (${seed.kind}, $clubId, NULL, ${intervalTrigger}, ${seed.intervalHours}, NULL, NULL, NULL, ${seed.enabled}, NULL)
            ON CONFLICT (kind, club_id) DO NOTHING""".update.run()
    }

  def insert(schedule: JobSchedule): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO job_schedule
              (kind, club_id, params, trigger_type, interval_hours, cron_expr, timezone, misfire_policy, enabled, last_run_at)
            VALUES (${schedule.kind}, ${schedule.clubId}, ${schedule.params}, ${schedule.triggerType},
                    ${schedule.intervalHours}, ${schedule.cronExpr}, ${schedule.timezone}, ${schedule.misfirePolicy},
                    ${schedule.enabled}, ${schedule.lastRunAt})
            RETURNING id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def updateLastRunAt(id: Long, at: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"UPDATE job_schedule SET last_run_at = $at WHERE id = $id".update.run()
    }

  /** Partial update of a schedule's mutable fields (never the trigger TYPE — callers guard that the supplied
    * fields match the row's existing type; the DB CHECK is the backstop). Each `Some` overwrites; `None`
    * preserves. `params` is `Option[Option[String]]`: outer `Some` means "set" (inner is the value, `None`
    * clears to NULL), outer `None` preserves.
    */
  def update(
    id: Long,
    intervalHours: Option[Short],
    cronExpr: Option[String],
    timezone: Option[String],
    misfirePolicy: Option[MisfirePolicy],
    enabled: Option[Boolean],
    params: Option[Option[String]]
  ): ZIO[PostgresClient, SQLException, Int] =
    if (
      intervalHours.isEmpty && cronExpr.isEmpty && timezone.isEmpty
      && misfirePolicy.isEmpty && enabled.isEmpty && params.isEmpty
    ) { ZIO.succeed(0) }
    else {
      val hasParams   = params.isDefined
      val paramsValue = params.flatten
      connectZIO {
        sql"""UPDATE job_schedule SET
                interval_hours = COALESCE($intervalHours, interval_hours),
                cron_expr      = COALESCE($cronExpr, cron_expr),
                timezone       = COALESCE($timezone, timezone),
                misfire_policy = COALESCE($misfirePolicy, misfire_policy),
                enabled        = COALESCE($enabled, enabled),
                params         = CASE WHEN $hasParams THEN $paramsValue ELSE params END
              WHERE id = $id""".update.run()
      }
    }

  def delete(id: Long): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM job_schedule WHERE id = $id".update.run()
    }

  /** Deletes all per-club schedule rows for one club (any kind). Used on unmanage (#106) so a club's
    * History/Membership — and any user-created per-club — schedules stop firing once it is no longer managed.
    * Global rows (`club_id IS NULL`) are untouched. Returns rows deleted.
    */
  def deleteByClub(clubId: ClubId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM job_schedule WHERE club_id = $clubId".update.run()
    }
}
