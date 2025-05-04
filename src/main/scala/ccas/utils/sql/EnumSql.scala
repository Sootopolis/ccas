package ccas.utils.sql

import io.getquill.MappedEncoding
import zio.json.{PascalCase, SnakeCase}

trait EnumSql[T <: reflect.Enum](
  encodingNaming: String => String = SnakeCase,
  decodingNaming: String => String = PascalCase
) {
  protected def valueOf(string: String): T

  given MappedEncoding[T, String] = MappedEncoding(member => encodingNaming(member.toString))
  given MappedEncoding[String, T] = MappedEncoding(string => valueOf(decodingNaming(string)))
}
