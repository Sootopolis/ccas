package ccas.server.routes

import com.augustnagro.magnum.Transactor
import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec}
import zio.ZIO

import ccas.api.misc.subtypes.ClubUrlName
import ccas.server.jobs.JobKind
import ccas.server.routes.RouteHelpers.*
import ccas.server.scheduler.JobSchedule

object ScheduleRoutes {

  // --- Request/response types ---

  case class CreateScheduleRequest(
    kind: String,
    clubUrlName: Option[String],
    params: Option[String],
    intervalHours: Int
  )
  object CreateScheduleRequest {
    given JsonCodec[CreateScheduleRequest] = DeriveJsonCodec.gen
  }

  case class UpdateScheduleRequest(intervalHours: Option[Int], enabled: Option[Boolean], params: Option[String])
  object UpdateScheduleRequest {
    given JsonCodec[UpdateScheduleRequest] = DeriveJsonCodec.gen
  }

  case class ScheduleResponse(
    id: Long,
    kind: String,
    clubUrlName: Option[String],
    params: Option[String],
    intervalHours: Int,
    enabled: Boolean,
    lastRunAt: Option[String]
  )
  object ScheduleResponse {
    given JsonCodec[ScheduleResponse] = DeriveJsonCodec.gen

    def fromSchedule(s: JobSchedule): ScheduleResponse =
      ScheduleResponse(
        id = s.id,
        kind = s.kind.toString,
        clubUrlName = s.clubUrlName.map(ClubUrlName.unwrap),
        params = s.params,
        intervalHours = s.intervalHours,
        enabled = s.enabled,
        lastRunAt = s.lastRunAt.map(_.toString)
      )
  }

  // --- Helpers ---

  private def parseJobKind(s: String): Either[String, JobKind] =
    scala.util.Try(JobKind.valueOf(s)).toEither.left.map(_ =>
      s"Invalid job kind: $s. Valid: ${JobKind.values.mkString(", ")}"
    )

  // --- Routes ---

  val routes: Routes[Transactor, Nothing] = Routes(
    Method.GET / "api" / "schedules" -> handler {
      JobSchedule.selectAll
        .map(list => jsonResponse(Status.Ok, list.map(ScheduleResponse.fromSchedule)))
        .catchAll(e => ZIO.succeed(jsonResponse(Status.InternalServerError, ErrorResponse(e.getMessage))))
    },
    Method.POST / "api" / "schedules" -> handler { (req: Request) =>
      (for {
        body <- parseJsonBody[CreateScheduleRequest](req)
        kind <- ZIO.fromEither(parseJobKind(body.kind)).mapError(e => new Exception(e))
        clubUrlName = body.clubUrlName.map(ClubUrlName.wrap)
        schedule = JobSchedule(0L, kind, clubUrlName, body.params, body.intervalHours, enabled = true, lastRunAt = None)
        id      <- JobSchedule.insert(schedule)
        created <- JobSchedule.selectId(id).someOrFail(new Exception("Failed to read back schedule"))
      } yield jsonResponse(Status.Created, ScheduleResponse.fromSchedule(created)))
        .catchAll(e => ZIO.succeed(jsonResponse(Status.InternalServerError, ErrorResponse(e.getMessage))))
    },
    Method.PUT / "api" / "schedules" / long("id") -> handler { (id: Long, req: Request) =>
      (for {
        body    <- parseJsonBody[UpdateScheduleRequest](req)
        _       <- JobSchedule.update(id, body.intervalHours, body.enabled, body.params.map(Some(_)))
        updated <- JobSchedule.selectId(id).someOrFail(new Exception(s"Schedule $id not found"))
      } yield jsonResponse(Status.Ok, ScheduleResponse.fromSchedule(updated)))
        .catchAll(e => ZIO.succeed(jsonResponse(Status.InternalServerError, ErrorResponse(e.getMessage))))
    },
    Method.DELETE / "api" / "schedules" / long("id") -> handler { (id: Long, _: Request) =>
      JobSchedule.delete(id)
        .as(Response(status = Status.NoContent))
        .catchAll(e => ZIO.succeed(jsonResponse(Status.InternalServerError, ErrorResponse(e.getMessage))))
    }
  )
}
