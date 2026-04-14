package ccas.server.routes

import zio.{Task, ZIO}
import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

import ccas.server.jobs.JobConflictException
import ccas.utils.client.HttpStatusException
import ccas.utils.errors.{BadRequestException, NotFoundException, UserFacingException}

object RouteHelpers {

  final case class ErrorResponse(error: String)

  object ErrorResponse {
    given JsonCodec[ErrorResponse] = DeriveJsonCodec.gen
  }

  def jsonResponse[A: JsonEncoder](status: Status, body: A): Response =
    Response.json(summon[JsonEncoder[A]].encodeJson(body, None).toString).status(status)

  def parseJsonBody[T: JsonDecoder](req: Request): Task[T] =
    req.body.asString.flatMap(s =>
      ZIO.fromEither(summon[JsonDecoder[T]].decodeJson(s)).mapError(e => new BadRequestException(e))
    )

  def withErrorHandling[R](effect: ZIO[R, Throwable, Response]): ZIO[R, Nothing, Response] =
    effect.catchAll(e => ZIO.succeed(handleError(e)))

  def handleError(error: Throwable): Response = error match {
    case e: HttpStatusException  => jsonResponse(Status.BadGateway, ErrorResponse(e.getMessage))
    case e: JobConflictException => jsonResponse(Status.Conflict, ErrorResponse(e.getMessage))
    case e: NotFoundException    => jsonResponse(Status.NotFound, ErrorResponse(e.getMessage))
    case e: BadRequestException  => jsonResponse(Status.BadRequest, ErrorResponse(e.getMessage))
    case e: UserFacingException  => jsonResponse(Status.BadRequest, ErrorResponse(e.getMessage))
    case _                       => jsonResponse(Status.InternalServerError, ErrorResponse("Internal server error"))
  }
}
