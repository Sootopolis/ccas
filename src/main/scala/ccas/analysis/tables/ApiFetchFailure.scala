package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class ApiFetchFailure(
    occurredAt: Instant,
    url: String,
    errorType: String,
    errorMessage: Option[String],
    responseBody: Option[String]
) derives DbCodec

object ApiFetchFailure {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS api_fetch_failure (
              failure_id     BIGSERIAL PRIMARY KEY,
              occurred_at    TIMESTAMPTZ NOT NULL,
              url            VARCHAR NOT NULL,
              error_type     VARCHAR NOT NULL,
              error_message  VARCHAR,
              response_body  VARCHAR
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_api_fetch_failure_occurred_at
            ON api_fetch_failure (occurred_at)""".update.run()
    }

  def insert(item: ApiFetchFailure): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO api_fetch_failure (occurred_at, url, error_type, error_message, response_body)
            VALUES (${item.occurredAt}, ${item.url}, ${item.errorType}, ${item.errorMessage}, ${item.responseBody})""".update
        .run()
    }

  def selectRecent(since: Instant): ZIO[Transactor, SQLException, List[ApiFetchFailure]] =
    connectZIO {
      sql"""SELECT occurred_at, url, error_type, error_message, response_body
            FROM api_fetch_failure WHERE occurred_at >= $since"""
        .query[ApiFetchFailure].run().toList
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM api_fetch_failure".update.run()
    }
}
