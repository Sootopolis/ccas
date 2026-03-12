package ccas.utils.sql

import com.augustnagro.magnum.DbCodec

trait EnumSql[T <: reflect.Enum] {
  protected def valueOf(string: String): T

  protected def encodingNaming(scalaString: String): String = scalaString
  protected def decodingNaming(sqlString: String): String   = sqlString

  given DbCodec[T] = DbCodec[String].biMap(
    string => if string == null then null.asInstanceOf[T] else valueOf(decodingNaming(string)),
    member => encodingNaming(member.toString)
  )
}
