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

  /** A list as a PostgreSQL array. One whose element has no `ArrayElement` gets no codec, so Magnum reports a missing
    * `DbCodec` for the list.
    */
  given listCodec[T: {DbCodec as dbCodec, ArrayElement as element}]: DbCodec[List[T]] = new DbCodec[List[T]] {
    override def cols: IArray[Int] = IArray(Types.ARRAY)

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
      val arr = ps.getConnection.createArrayOf(element.sqlTypeName, value.map(element.toJdbc).toArray)
      ps.setArray(pos, arr)

    override def queryRepr: String = "?"
  }
}
