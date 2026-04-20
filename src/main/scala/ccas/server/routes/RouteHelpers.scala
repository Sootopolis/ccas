package ccas.server.routes

import zio.{Task, ZIO}
import zio.http.*
import zio.json.{JsonDecoder, JsonEncoder}

import ccas.utils.client.HttpStatusException
import ccas.utils.errors.{BadRequestException, ErrorResponse, UserFacingError}

object RouteHelpers {

  def jsonResponse[A: JsonEncoder](status: Status, body: A): Response =
    Response.json(summon[JsonEncoder[A]].encodeJson(body, None).toString).status(status)

  def parseJsonBody[T: JsonDecoder](req: Request): Task[T] =
    req.body.asString.flatMap(s =>
      ZIO.fromEither(summon[JsonDecoder[T]].decodeJson(s)).mapError(BadRequestException(_))
    )

  def withErrorHandling[R](effect: ZIO[R, Throwable, Response]): ZIO[R, Nothing, Response] =
    effect.catchAll(e => ZIO.succeed(handleError(e)))

  def handleError(error: Throwable): Response = error match {
    case e: HttpStatusException => jsonResponse(Status.BadGateway, ErrorResponse(e.getMessage))
    case e: UserFacingError     => Response.json(e.renderBody).status(e.status)
    case _                      => jsonResponse(Status.InternalServerError, ErrorResponse("Internal server error"))
  }
}
