package ccas.utils.sql

import java.sql.{PreparedStatement, ResultSet, Types}
import java.time.Instant

import com.augustnagro.magnum.DbCodec
import zio.http.URL

object DbCodecs {
  given DbCodec[Instant] = new DbCodec[Instant] {
    override def cols: IArray[Int] = IArray(Types.TIMESTAMP_WITH_TIMEZONE)
    override def readSingle(rs: ResultSet, pos: Int): Instant =
      val odt = rs.getObject(pos, classOf[java.time.OffsetDateTime])
      if (odt == null) { null }
      else { odt.toInstant }
    override def writeSingle(value: Instant, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, value.atOffset(java.time.ZoneOffset.UTC))
    override def queryRepr: String = "?"
  }

  given DbCodec[URL] = DbCodec[String].biMap(
    string =>
      if (string == null) { null }
      else {
        URL.decode(string).fold(
          _ => throw new IllegalStateException(s"Malformed URL in database: $string"),
          identity
        )
      },
    url =>
      if (url == null) { null }
      else { url.encode }
  )

  given listCodec[T: DbCodec as dbCodec]: DbCodec[List[T]] = new DbCodec[List[T]] {
    override def cols: IArray[Int] = IArray(Types.ARRAY)

    private val (sqlTypeName, toJdbc) = dbCodec.cols(0) match {
      case Types.VARCHAR | Types.LONGVARCHAR => ("TEXT", (v: T) => v.asInstanceOf[AnyRef])
      case Types.BIGINT                      => ("BIGINT", (v: T) => Long.box(v.asInstanceOf[Long]))
      case Types.INTEGER                     => ("INTEGER", (v: T) => Int.box(v.asInstanceOf[Int]))
      case other => throw IllegalStateException(s"Unsupported array element JDBC type: $other")
    }

    override def readSingle(rs: ResultSet, pos: Int): List[T] =
      val arr = rs.getArray(pos)
      if (arr == null) { Nil }
      else {
        val innerRs = arr.getResultSet
        val builder = List.newBuilder[T]
        while (innerRs.next())
          builder += dbCodec.readSingle(innerRs, 2)
        innerRs.close()
        builder.result()
      }

    override def writeSingle(value: List[T], ps: PreparedStatement, pos: Int): Unit =
      val arr = ps.getConnection.createArrayOf(sqlTypeName, value.map(toJdbc).toArray)
      ps.setArray(pos, arr)

    override def queryRepr: String = "?"
  }
}
