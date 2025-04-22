package ccas.utils.sql

import io.getquill.MappedEncoding
import zio.json.{PascalCase, SnakeCase}

trait EnumSql[T <: reflect.Enum] {
  protected def valueOf(string: String): T

  given MappedEncoding[T, String] = MappedEncoding(member => SnakeCase(member.toString))

  given MappedEncoding[String, T] = MappedEncoding(string => valueOf(PascalCase(string)))
}
