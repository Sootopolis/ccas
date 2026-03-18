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
      if odt == null then null else odt.toInstant
    override def writeSingle(value: Instant, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, value.atOffset(java.time.ZoneOffset.UTC))
    override def queryRepr: String = "?"
  }

  given DbCodec[URL] = DbCodec[String].biMap(
    string =>
      if string == null then null
      else
        URL.decode(string).fold(
          _ => throw new IllegalStateException(s"Malformed URL in database: $string"),
          identity
        )
    ,
    url => if url == null then null else url.encode
  )

  given DbCodec[List[String]] = new DbCodec[List[String]] {
    override def cols: IArray[Int] = IArray(Types.ARRAY)
    override def readSingle(rs: ResultSet, pos: Int): List[String] =
      val arr = rs.getArray(pos)
      if arr == null then Nil
      // safe: JDBC Array.getArray returns Object[] for TEXT[] columns
      else arr.getArray.asInstanceOf[Array[AnyRef]].map(_.toString).toList
    override def writeSingle(value: List[String], ps: PreparedStatement, pos: Int): Unit =
      val arr = ps.getConnection.createArrayOf("TEXT", value.toArray[AnyRef])
      ps.setArray(pos, arr)
    override def queryRepr: String = "?"
  }
}
