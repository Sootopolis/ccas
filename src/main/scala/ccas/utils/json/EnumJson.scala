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

  final def encode(t: T): String = t.toJsonPretty

  final def encodeZIO(t: T): UIO[String] = ZIO.succeed(encode(t))

  final def decode(string: String): Either[String, T] = string.fromJson[T]

  final def decodeZIO(string: String): IO[JsonDecodingException, T] =
    ZIO.fromEither(decode(string)).mapError(JsonDecodingException(_))

  extension (member: T) {
    def encodeJson: String = member.toJsonPretty
    def encodeJsonPretty: String = member.toJsonPretty
  }

  extension (string: String) {
    def decodeJson: Either[String, T] = string.fromJson[T]
    def decodeJsonZIO: IO[JsonDecodingException, T] = ZIO.fromEither(decodeJson).mapError(JsonDecodingException(_))
  }
}
