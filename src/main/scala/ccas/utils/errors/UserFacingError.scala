package ccas.utils.errors

import zio.{IO, ZIO}
import zio.http.Status
import zio.json.{DeriveJsonCodec, EncoderOps, JsonCodec, JsonEncoder}

final case class ErrorResponse(error: String)

object ErrorResponse {
  given JsonCodec[ErrorResponse] = DeriveJsonCodec.gen
}

/** Marker for errors whose `getMessage` and response body are intentionally safe to surface to users.
  *
  * HTTP routes render instances via `RouteHelpers.renderError` using `status` and `renderBody`. CLI apps use the
  * inherited `getMessage` only — the JSON body is never computed on that path thanks to the `lazy val` in each variant.
  */
sealed trait UserFacingError extends Throwable {
  def status: Status
  def renderBody: String
}

final class BadRequestException private (msg: String, body: => String) extends Exception(msg) with UserFacingError {
  val status          = Status.BadRequest
  lazy val renderBody = body
}

object BadRequestException {
  def apply(msg: String): BadRequestException =
    new BadRequestException(msg, ErrorResponse(msg).toJson)
  def of[B: JsonEncoder](body: B, msg: String): BadRequestException =
    new BadRequestException(msg, body.toJson)

  /** Refuses a request field `key` set below `min`; an absent one passes. */
  def requireAtLeast(key: String, min: Int, valueOption: Option[Int]): IO[BadRequestException, Unit] =
    ZIO.foreachDiscard(valueOption.filter(_ < min)) { value =>
      ZIO.fail(BadRequestException(s"'$key' must be at least $min (got $value)"))
    }
}

final class NotFoundException private (msg: String, body: => String) extends Exception(msg) with UserFacingError {
  val status          = Status.NotFound
  lazy val renderBody = body
}

object NotFoundException {
  def apply(msg: String): NotFoundException =
    new NotFoundException(msg, ErrorResponse(msg).toJson)
  def of[B: JsonEncoder](body: B, msg: String): NotFoundException =
    new NotFoundException(msg, body.toJson)
}

final class ConflictException private (msg: String, body: => String) extends Exception(msg) with UserFacingError {
  val status          = Status.Conflict
  lazy val renderBody = body
}

object ConflictException {
  def apply(msg: String): ConflictException =
    new ConflictException(msg, ErrorResponse(msg).toJson)
  def of[B: JsonEncoder](body: B, msg: String): ConflictException =
    new ConflictException(msg, body.toJson)
}
