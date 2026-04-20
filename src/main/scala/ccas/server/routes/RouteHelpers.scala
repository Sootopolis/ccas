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

  /** Route boundary error handler. Expected user-facing errors render directly from the sealed hierarchy. Escaping
    * `HttpStatusException`s (unhandled upstream Chess.com failures) still render 502 for caller ergonomics. Anything
    * else — including defects — collapses to a generic 500 with the full cause logged at error level, so operational
    * visibility isn't lost. Pure interruption causes are re-propagated rather than rendered, matching the old
    * `catchAll`-based behaviour and keeping shutdown / client-disconnect noise out of the error log.
    */
  def withErrorHandling[R](effect: ZIO[R, Throwable, Response]): ZIO[R, Nothing, Response] =
    effect
      .catchSome {
        case e: UserFacingError     => ZIO.succeed(Response.json(e.renderBody).status(e.status))
        case e: HttpStatusException => ZIO.succeed(jsonResponse(Status.BadGateway, ErrorResponse(e.getMessage)))
      }
      .catchAllCause { cause =>
        if (cause.isInterruptedOnly) { ZIO.interrupt }
        else {
          ZIO.logErrorCause("Unhandled error in route", cause)
            .as(jsonResponse(Status.InternalServerError, ErrorResponse("Internal server error")))
        }
      }
}
