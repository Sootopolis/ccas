package ccas.server.routes

import zio.{Cause, Task, UIO, ZIO}
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

  /** Route boundary error renderer; attach it to every route table with `.handleErrorRequestCauseZIO(renderError)`.
    * The fallback 500 is generic so nothing unvetted reaches the caller, and logs the full cause so nothing is lost to
    * the operator. A pure interruption never arrives here: zio-http re-propagates it first, keeping shutdown and
    * client-disconnect noise out of the log.
    */
  def renderError(request: Request, cause: Cause[Throwable]): UIO[Response] =
    cause.failureOption match {
      case Some(e: UserFacingError)     => ZIO.succeed(Response.json(e.renderBody).status(e.status))
      case Some(e: HttpStatusException) => ZIO.succeed(jsonResponse(Status.BadGateway, ErrorResponse(e.getMessage)))
      case _ =>
        ZIO.logErrorCause(s"Unhandled error in route ${request.method.name} ${request.path.encode}", cause)
          .as(jsonResponse(Status.InternalServerError, ErrorResponse("Internal server error")))
    }
}
