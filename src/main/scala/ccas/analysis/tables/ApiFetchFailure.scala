package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, withTransaction}

final case class ApiFetchFailure(
  occurredAt: Instant,
  url: String,
  errorType: String,
  errorMessage: Option[String],
  responseBody: Option[String]
) derives DbCodec

object ApiFetchFailure {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS api_fetch_failure (
              failure_id       BIGSERIAL PRIMARY KEY,
              occurred_at      TIMESTAMPTZ NOT NULL,
              url              TEXT NOT NULL,
              error_type       TEXT NOT NULL,
              error_message    TEXT,
              response_body_id BIGINT REFERENCES api_response_body (body_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_api_fetch_failure_occurred_at
            ON api_fetch_failure (occurred_at)""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_api_fetch_failure_response_body_id
            ON api_fetch_failure (response_body_id)""".update.run()
    }

  def selectRecent(since: Instant): ZIO[PostgresClient, SQLException, List[ApiFetchFailure]] =
    connectZIO {
      sql"""SELECT f.occurred_at, f.url, f.error_type, f.error_message, b.body
            FROM api_fetch_failure f
            LEFT JOIN api_response_body b ON b.body_id = f.response_body_id
            WHERE f.occurred_at >= $since
            ORDER BY f.occurred_at DESC"""
        .query[ApiFetchFailure].run().toList
    }

  def insert(item: ApiFetchFailure): ZIO[PostgresClient, SQLException, Int] =
    withTransaction {
      for {
        bodyIdOpt <- ZIO.foreach(item.responseBody)(ApiResponseBody.ensureBody)
        result <- connectZIO {
          sql"""INSERT INTO api_fetch_failure (occurred_at, url, error_type, error_message, response_body_id)
                VALUES (${item.occurredAt}, ${item.url}, ${item.errorType}, ${item.errorMessage}, $bodyIdOpt)""".update
            .run()
        }
      } yield result
    }

  def deleteBefore(cutoff: Instant): ZIO[PostgresClient, SQLException, Int] =
    withTransaction {
      connectZIO {
        sql"DELETE FROM api_fetch_failure WHERE occurred_at < $cutoff".update.run()
      }.flatMap { count =>
        ApiResponseBody.deleteOrphans.as(count)
      }
    }
}
