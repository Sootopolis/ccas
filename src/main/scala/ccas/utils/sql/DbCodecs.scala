package ccas.utils.sql

import com.augustnagro.magnum.DbCodec

import java.sql.{PreparedStatement, ResultSet, Types}
import java.time.Instant

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
}
