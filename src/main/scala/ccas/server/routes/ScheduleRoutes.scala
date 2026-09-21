package ccas.server.routes

import java.time.Instant

import scala.util.chaining.*
import scala.util.Try

import zio.http.*
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec, SnakeCase}
import zio.{Clock, RIO, ZIO}

import ccas.analysis.apps.{ClubQuery, ClubResolution}
import ccas.api.misc.subtypes.ClubId
import ccas.server.jobs.JobKind
import ccas.server.routes.RouteHelpers.*
import ccas.server.scheduler.{JobSchedule, MisfirePolicy, ScheduleTrigger, TriggerType}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient

object ScheduleRoutes {

  // --- Request/response types ---
  //
  // A schedule is either interval- or cron-triggered. The request is backward compatible: a legacy body with
  // only `intervalHours` (no `triggerType`) decodes the new Option fields as None and defaults to Interval.
  // `triggerType`/`misfire` are typed enums decoded snake_case via their EnumJson codecs (`interval`/`cron`,
  // `skip`/`catch_up`); `cron` is the 5-field unix expression users type.

  private[ccas] case class CreateScheduleRequest(
    kind: String,
    @jsonField("club") clubOption: Option[ClubQuery],
    params: Option[String],
    triggerType: Option[TriggerType],
    intervalHours: Option[Int],
    cron: Option[String],
    timezone: Option[String],
    misfire: Option[MisfirePolicy]
  )
  object CreateScheduleRequest {
    given JsonCodec[CreateScheduleRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class UpdateScheduleRequest(
    intervalHours: Option[Int],
    cron: Option[String],
    timezone: Option[String],
    misfire: Option[MisfirePolicy],
    enabled: Option[Boolean],
    params: Option[String]
  )
  object UpdateScheduleRequest {
    given JsonCodec[UpdateScheduleRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class ScheduleResponse(
    id: Long,
    kind: String,
    @jsonField("clubId") clubIdOption: Option[Long],
    params: Option[String],
    triggerType: String,
    intervalHours: Option[Int],
    cron: Option[String],
    timezone: Option[String],
    misfire: Option[String],
    enabled: Boolean,
    lastRunAt: Option[String]
  )
  object ScheduleResponse {
    given JsonCodec[ScheduleResponse] = DeriveJsonCodec.gen

    def fromSchedule(s: JobSchedule): ScheduleResponse =
      ScheduleResponse(
        id = s.id,
        kind = s.kind.toString,
        clubIdOption = s.clubIdOption.map(ClubId.unwrap),
        params = s.params,
        triggerType = SnakeCase(s.triggerType.toString),
        intervalHours = s.intervalHours.map(_.toInt),
        cron = s.cronExpr.map(denormalizeCron),
        timezone = s.timezone,
        misfire = s.misfirePolicy.map(mp => SnakeCase(mp.toString)),
        enabled = s.enabled,
        lastRunAt = s.lastRunAt.map(_.toString)
      )
  }

  // A schedule for a named club is created only once that name resolves, the way a submit runs only then; the
  // resolution comes back either way, and is absent exactly when no club was named.
  private[ccas] case class CreateScheduleResponse(
    @jsonField("schedule") scheduleOption: Option[ScheduleResponse],
    @jsonField("resolution") resolutionOption: Option[ClubResolution]
  )
  object CreateScheduleResponse {
    given JsonCodec[CreateScheduleResponse] = DeriveJsonCodec.gen
  }

  // --- Helpers ---

  private def parseJobKind(s: String): Either[String, JobKind] =
    Try(JobKind.valueOf(s)).toEither.left.map(_ =>
      s"Invalid job kind: $s. Valid: ${JobKind.values.mkString(", ")}"
    )

  /** Render the stored 6-field cron back toward the user's 5-field input: drop the leading seconds field and map the
    * injected `?` day-field back to `*` (the form they typed). `?` and `*` are equivalent once the other day field
    * carries the constraint, so this round-trips through [[ScheduleTrigger.validateCron]].
    */
  private def denormalizeCron(stored: String): String =
    stored.trim.split("\\s+").toList match {
      case _ :: rest => rest.map(f => if (f == "?") "*" else f).mkString(" ")
      case Nil       => stored
    }

  private val intervalRangeMsg = s"intervalHours must be between 1 and ${Short.MaxValue}"

  /** Arguments for [[JobSchedule.update]], built by [[buildUpdate]] after trigger-shape validation. */
  private case class UpdateArgs(
    intervalHours: Option[Short],
    cronExpr: Option[String],
    timezone: Option[String],
    misfirePolicy: Option[MisfirePolicy],
    enabled: Option[Boolean],
    params: Option[Option[String]]
  )

  /** Pure validation of a create request into a row naming no club, which the caller fills in once the request's club
    * resolves — only after this passes, since resolving may ask Chess.com and record its answer. `now` stamps a cron
    * row's `last_run_at` so its first fire is the next boundary (no backfire).
    */
  private def buildCreate(kind: JobKind, body: CreateScheduleRequest, now: Instant): Either[String, JobSchedule] =
    body.triggerType.getOrElse(TriggerType.Interval) match {
      case TriggerType.Interval =>
        for {
          _ <- Either.cond(
            body.cron.isEmpty && body.timezone.isEmpty && body.misfire.isEmpty,
            (),
            "interval trigger takes only intervalHours, not cron/timezone/misfire"
          )
          ih <- body.intervalHours.toRight("intervalHours is required for an interval trigger")
          _  <- Either.cond(ih > 0 && ih <= Short.MaxValue, (), intervalRangeMsg)
        } yield JobSchedule.interval(
          id = 0L,
          kind = kind,
          clubIdOption = None,
          params = body.params,
          intervalHours = ih.toShort,
          enabled = true,
          lastRunAt = None
        )

      case TriggerType.Cron =>
        for {
          _    <- Either.cond(body.intervalHours.isEmpty, (), "cron trigger does not take intervalHours")
          raw  <- body.cron.toRight("cron is required for a cron trigger")
          norm <- ScheduleTrigger.validateCron(raw)
          tz   <- ScheduleTrigger.validateZone(body.timezone.getOrElse("UTC"))
          misfire = body.misfire.getOrElse(MisfirePolicy.Skip)
        } yield JobSchedule.cron(
          id = 0L,
          kind = kind,
          clubIdOption = None,
          params = body.params,
          cronExpr = norm,
          timezone = tz,
          misfire = misfire,
          enabled = true,
          lastRunAt = Some(now)
        )
    }

  /** Pure validation of an update against an existing row's trigger type. Switching trigger type via PUT is out of
    * scope (delete + recreate); a field that doesn't match the row's type is a 400. Returns the [[UpdateArgs]] passed
    * to [[JobSchedule.update]].
    */
  private def buildUpdate(existing: JobSchedule, body: UpdateScheduleRequest): Either[String, UpdateArgs] =
    existing.triggerType match {
      case TriggerType.Interval =>
        for {
          _ <- Either.cond(
            body.cron.isEmpty && body.timezone.isEmpty && body.misfire.isEmpty,
            (),
            "cannot set cron/timezone/misfire on an interval schedule"
          )
          _ <- Either.cond(body.intervalHours.forall(h => h > 0 && h <= Short.MaxValue), (), intervalRangeMsg)
        } yield UpdateArgs(body.intervalHours.map(_.toShort), None, None, None, body.enabled, body.params.map(Some(_)))

      case TriggerType.Cron =>
        for {
          _        <- Either.cond(body.intervalHours.isEmpty, (), "cannot set intervalHours on a cron schedule")
          normCron <- body.cron.fold[Either[String, Option[String]]](Right(None))(c => ScheduleTrigger.validateCron(c).map(Some(_)))
          tz       <- body.timezone.fold[Either[String, Option[String]]](Right(None))(z => ScheduleTrigger.validateZone(z).map(Some(_)))
        } yield UpdateArgs(None, normCron, tz, body.misfire, body.enabled, body.params.map(Some(_)))
    }

  private def persist(schedule: JobSchedule): RIO[PostgresClient, ScheduleResponse] =
    for {
      id      <- JobSchedule.insert(schedule)
      created <- JobSchedule.selectId(id).someOrFail(new Exception("Failed to read back schedule"))
    } yield ScheduleResponse.fromSchedule(created)

  // --- Routes ---

  val routes: Routes[ChessComClient & PostgresClient, Nothing] = Routes(
    Method.GET / "api" / "schedules" -> handler {
      JobSchedule.selectAll
        .map(list => jsonResponse(Status.Ok, list.map(ScheduleResponse.fromSchedule)))
        .pipe(withErrorHandling)
    },
    Method.POST / "api" / "schedules" -> handler { (req: Request) =>
      (for {
        body             <- parseJsonBody[CreateScheduleRequest](req)
        kind             <- ZIO.fromEither(parseJobKind(body.kind)).mapError(BadRequestException(_))
        now              <- Clock.instant
        clubless         <- ZIO.fromEither(buildCreate(kind, body, now)).mapError(BadRequestException(_))
        resolutionOption <- ZIO.foreach(body.clubOption)(ClubRequest.resolve)
        scheduleOption <- resolutionOption.map(_.runnable) match {
          case Some(Left(_))     => ZIO.none
          case Some(Right(club)) => persist(clubless.copy(clubIdOption = Some(club.clubId))).asSome
          case None              => persist(clubless).asSome
        }
        status = if (scheduleOption.isDefined) { Status.Created } else { Status.Ok }
      } yield jsonResponse(status, CreateScheduleResponse(scheduleOption, resolutionOption)))
        .pipe(withErrorHandling)
    },
    Method.PUT / "api" / "schedules" / long("id") -> handler { (id: Long, req: Request) =>
      (for {
        body     <- parseJsonBody[UpdateScheduleRequest](req)
        existing <- JobSchedule.selectId(id).someOrFail(NotFoundException(s"Schedule $id not found"))
        args     <- ZIO.fromEither(buildUpdate(existing, body)).mapError(BadRequestException(_))
        _ <- JobSchedule.update(
          id,
          args.intervalHours,
          args.cronExpr,
          args.timezone,
          args.misfirePolicy,
          args.enabled,
          args.params
        )
        updated <- JobSchedule.selectId(id).someOrFail(NotFoundException(s"Schedule $id not found"))
      } yield jsonResponse(Status.Ok, ScheduleResponse.fromSchedule(updated)))
        .pipe(withErrorHandling)
    },
    Method.DELETE / "api" / "schedules" / long("id") -> handler { (id: Long, _: Request) =>
      JobSchedule.delete(id)
        .as(Response(status = Status.NoContent))
        .pipe(withErrorHandling)
    }
  )
}
