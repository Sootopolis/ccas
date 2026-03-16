package ccas.utils.json

import scala.util.Try

import zio.{IO, UIO, ZIO}
import zio.json.{DecoderOps, EncoderOps, JsonCodec, JsonDecoder, PascalCase, SnakeCase}

trait EnumJson[T <: scala.reflect.Enum] extends StringDecodeOps[T] {
  protected def valueOf(string: String): T

  protected def enumToJson(member: T): String = SnakeCase(member.toString)

  protected def jsonToEnum(string: String): Either[String, T] =
    Try(valueOf(PascalCase(string))).toEither.left.map(_.getMessage)

  final given jsonCodec: JsonCodec[T] = JsonCodec.string.transformOrFail(jsonToEnum, enumToJson)

  override protected def jsonDecoderInstance: JsonDecoder[T] = jsonCodec.decoder

  protected def lookupJson(map: Map[String, T]): String => Either[String, T] =
    string => map.get(string).toRight(s"Invalid enum value: $string")

  final def encode(t: T): String = t.toJsonPretty

  final def encodeZIO(t: T): UIO[String] = ZIO.succeed(encode(t))

  final def decode(string: String): Either[String, T] = string.fromJson[T]

  final def decodeZIO(string: String): IO[JsonDecodingException, T] =
    ZIO.fromEither(decode(string)).mapError(JsonDecodingException(_))

  extension (member: T) {
    def encodeJson: String       = member.toJson
    def encodeJsonPretty: String = member.toJsonPretty
  }

}
