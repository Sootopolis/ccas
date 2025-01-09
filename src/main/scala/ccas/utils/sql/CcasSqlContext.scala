package ccas.utils.sql

import io.getquill.dsl.DateOps
import io.getquill.{PostgresZioJdbcContext, SnakeCase}
import zio.http.URL

import java.sql.Types
import java.time.Instant

object CcasSqlContext extends PostgresZioJdbcContext(SnakeCase) with DateOps {
  implicit val urlDecoder: Decoder[URL] =
    decoder(resultRow => index => URL.decode(resultRow.getString(index)).fold(throw _, identity))

  implicit val urlEncoder: Encoder[URL] =
    encoder(Types.VARCHAR, (index, url, prepareRow) => prepareRow.setString(index, url.encode))

  override implicit val instantDecoder: Decoder[Instant] =
    decoder(resultRow => index => Instant.ofEpochSecond(resultRow.getLong(index)))

  override implicit val instantEncoder: Encoder[Instant] =
    encoder(Types.BIGINT, (index, instant, prepareRow) => prepareRow.setLong(index, instant.getEpochSecond))
}
