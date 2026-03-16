package ccas.server.routes

import zio.{Task, ZIO}
import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

import ccas.utils.errors.ExternalException

object RouteHelpers {

  case class ErrorResponse(error: String)
  object ErrorResponse {
    given JsonCodec[ErrorResponse] = DeriveJsonCodec.gen
  }

  def jsonResponse[A: JsonEncoder](status: Status, body: A): Response =
    Response.json(summon[JsonEncoder[A]].encodeJson(body, None).toString).status(status)

  def parseJsonBody[T: JsonDecoder](req: Request): Task[T] =
    req.body.asString.flatMap(s =>
      ZIO.fromEither(summon[JsonDecoder[T]].decodeJson(s)).mapError(e => new ExternalException(e))
    )
}
