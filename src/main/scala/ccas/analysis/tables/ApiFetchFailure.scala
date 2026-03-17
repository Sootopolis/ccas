package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class ApiFetchFailure(
    url: String,
    errorType: String,
    errorMessage: Option[String],
    occurredAt: Instant
) derives DbCodec

object ApiFetchFailure {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS api_fetch_failure (
              url          VARCHAR NOT NULL,
              error_type   VARCHAR NOT NULL,
              error_message VARCHAR,
              occurred_at  TIMESTAMPTZ NOT NULL
            )""".update.run()
    }

  def insert(item: ApiFetchFailure): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO api_fetch_failure (url, error_type, error_message, occurred_at)
            VALUES (${item.url}, ${item.errorType}, ${item.errorMessage}, ${item.occurredAt})""".update.run()
    }

  def selectRecent(since: Instant): ZIO[Transactor, SQLException, List[ApiFetchFailure]] =
    connectZIO {
      sql"""SELECT url, error_type, error_message, occurred_at
            FROM api_fetch_failure WHERE occurred_at >= $since"""
        .query[ApiFetchFailure].run().toList
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM api_fetch_failure".update.run()
    }
}
