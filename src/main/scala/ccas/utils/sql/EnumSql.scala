package ccas.utils.sql

import com.augustnagro.magnum.DbCodec

trait EnumSql[T <: reflect.Enum] {
  protected def valueOf(string: String): T

  protected def encodingNaming(scalaString: String): String = scalaString
  protected def decodingNaming(sqlString: String): String   = sqlString

  // null.asInstanceOf[T] is safe here: JDBC returns null for SQL NULL columns,
  // and Magnum's DbCodec protocol expects null passthrough for nullable fields.
  given DbCodec[T] = DbCodec[String].biMap(
    string => if string == null then null.asInstanceOf[T] else valueOf(decodingNaming(string)),
    member => encodingNaming(member.toString)
  )
}
