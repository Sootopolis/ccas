package ccas.utils.sql

import java.sql.{Array as SqlArray, PreparedStatement, ResultSet, Types}

import scala.util.Using

import com.augustnagro.magnum.DbCodec
import zio.http.URL

object DbCodecs {
  given DbCodec[URL] = DbCodec[String].biMap(
    to = string =>
      URL.decode(string).fold(
        _ => throw new IllegalStateException(s"Malformed URL in database: $string"),
        identity
      ),
    from = _.encode
  )

  given listCodec[T: DbCodec as dbCodec]: DbCodec[List[T]] = new DbCodec[List[T]] {
    override def cols: IArray[Int] = IArray(Types.ARRAY)

    private val (sqlTypeName, toJdbc) = dbCodec.cols(0) match {
      case Types.VARCHAR | Types.LONGVARCHAR => ("TEXT", (v: T) => v.asInstanceOf[AnyRef])
      case Types.BIGINT                      => ("BIGINT", (v: T) => Long.box(v.asInstanceOf[Long]))
      case Types.INTEGER                     => ("INTEGER", (v: T) => Int.box(v.asInstanceOf[Int]))
      case other => throw IllegalStateException(s"Unsupported array element JDBC type: $other")
    }

    private def decode(arr: SqlArray): List[T] =
      try {
        Using.resource(arr.getResultSet) { innerRs =>
          val builder = List.newBuilder[T]
          while (innerRs.next())
            builder += dbCodec.readSingle(innerRs, 2)
          builder.result()
        }
      } finally arr.free()

    override def readSingle(rs: ResultSet, pos: Int): List[T] =
      readSingleOption(rs, pos).getOrElse(Nil)

    override def readSingleOption(rs: ResultSet, pos: Int): Option[List[T]] =
      Option(rs.getArray(pos)).map(decode)

    override def writeSingle(value: List[T], ps: PreparedStatement, pos: Int): Unit =
      val arr = ps.getConnection.createArrayOf(sqlTypeName, value.map(toJdbc).toArray)
      ps.setArray(pos, arr)

    override def queryRepr: String = "?"
  }
}
