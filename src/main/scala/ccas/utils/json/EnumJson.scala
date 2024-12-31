package ccas.utils.json

import zio.{IO, UIO, ZIO}
import zio.json.{DecoderOps, EncoderOps, JsonCodec, PascalCase, SnakeCase}

import scala.util.Try

trait EnumJson[T <: scala.reflect.Enum] {
  protected def valueOf(string: String): T

  protected def enumToJson(member: T): String = SnakeCase(member.toString)

  protected def jsonToEnum(string: String): Either[String, T] =
    Try(valueOf(PascalCase(string))).toEither.left.map(_.getMessage)

  final given jsonCodec: JsonCodec[T] = JsonCodec.string.transformOrFail(jsonToEnum, enumToJson)

  final def encode(t: T): String = t.toJsonPretty(using jsonCodec.encoder)

  final def encodeZIO(t: T): UIO[String] = ZIO.succeed(encode(t))

  final def decode(string: String): Either[String, T] = string.fromJson[T](using jsonCodec.decoder)

  final def decodeZIO(string: String): IO[JsonDecodingException, T] =
    ZIO.fromEither(decode(string)).mapError(JsonDecodingException(_))
}
