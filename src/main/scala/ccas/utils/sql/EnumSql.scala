package ccas.utils.sql

import io.getquill.MappedEncoding

trait EnumSql[T <: reflect.Enum] {
  protected def valueOf(string: String): T

  protected def encodingNaming(scalaString: String): String = scalaString
  protected def decodingNaming(sqlString: String): String = sqlString

  given MappedEncoding[T, String] = MappedEncoding(member => encodingNaming(member.toString))
  given MappedEncoding[String, T] = MappedEncoding(string => valueOf(decodingNaming(string)))
}
