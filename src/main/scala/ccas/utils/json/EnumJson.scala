package ccas.utils.json

import scala.reflect.Enum
import scala.util.Try

import zio.json.{EncoderOps, JsonCodec, PascalCase, SnakeCase}

trait EnumJson[T <: Enum] {
  protected def valueOf(string: String): T

  protected def enumToJson(member: T): String = SnakeCase(member.toString)

  protected def jsonToEnum(string: String): Either[String, T] =
    Try(valueOf(PascalCase(string))).toEither.left.map(_.getMessage)

  final given jsonCodec: JsonCodec[T] = JsonCodec.string.transformOrFail(jsonToEnum, enumToJson)

  protected def lookupJson(map: Map[String, T]): String => Either[String, T] =
    string => map.get(string).toRight(s"Invalid enum value: $string")

  extension (member: T) {
    def encodeJson: String = member.toJson
  }

}
