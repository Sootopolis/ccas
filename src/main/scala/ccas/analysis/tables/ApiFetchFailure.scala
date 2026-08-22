package ccas.analysis.tables

import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.client.BodyStore
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

  /** Positional decode target for `selectRecent`: the join yields the body's `body_hash`, not the body itself (bodies
    * live in the [[BodyStore]] since #191), so the body string is resolved in a second step.
    */
  private final case class FailureRow(
    occurredAt: Instant,
    url: String,
    errorType: String,
    errorMessage: Option[String],
    bodyHash: Option[String]
  ) derives DbCodec

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

  /** Recent failures with their response bodies. The join resolves each row's `body_hash`; the body bytes are then
    * loaded from the [[BodyStore]] (a missing object — or an unreachable store — yields `None`, e.g. a legacy row
    * whose body predates R2).
    */
  def selectRecent(since: Instant): ZIO[PostgresClient & BodyStore, SQLException, List[ApiFetchFailure]] =
    connectZIO {
      sql"""SELECT f.occurred_at, f.url, f.error_type, f.error_message, b.body_hash
            FROM api_fetch_failure f
            LEFT JOIN api_response_body b ON b.body_id = f.response_body_id
            WHERE f.occurred_at >= $since
            ORDER BY f.occurred_at DESC"""
        .query[FailureRow].run().toList
    }.flatMap { rows =>
      ZIO.foreach(rows) { row =>
        ZIO
          // `toOption`: this is an audit-trail display, so "the object is gone" and "the store is down" both mean
          // the same thing to the reader — a row whose body we cannot show. Cache repair is the only caller that
          // needs the distinction (see BodyRead).
          .foreach(row.bodyHash)(hash => BodyStore.read(hash).map(_.map(new String(_, StandardCharsets.UTF_8)).toOption))
          .map(bodyOpt => ApiFetchFailure(row.occurredAt, row.url, row.errorType, row.errorMessage, bodyOpt.flatten))
      }
    }

  /** Record a failed fetch. A [[BodyStore]] outage degrades to a body-less row rather than losing the failure
    * entirely — the audit trail is the point, the response body is a bonus.
    */
  def insert(item: ApiFetchFailure): ZIO[PostgresClient & BodyStore, SQLException, Int] =
    for {
      // Store the body BEFORE opening the transaction so the object-store round-trip never holds a pooled connection.
      hashOpt <- ZIO
        .foreach(item.responseBody)(body => ApiResponseBody.putBody(source = item.url, body = body))
        .map(_.flatten)
      result <- withTransaction {
        for {
          bodyIdOpt <- ZIO.foreach(hashOpt)(ApiResponseBody.ensureBodyPointer)
          inserted <- connectZIO {
            sql"""INSERT INTO api_fetch_failure (occurred_at, url, error_type, error_message, response_body_id)
                  VALUES (${item.occurredAt}, ${item.url}, ${item.errorType}, ${item.errorMessage}, $bodyIdOpt)""".update
              .run()
          }
        } yield inserted
      }
    } yield result

  def deleteBefore(cutoff: Instant): ZIO[PostgresClient & BodyStore, SQLException, Int] =
    withTransaction {
      for {
        count  <- connectZIO(sql"DELETE FROM api_fetch_failure WHERE occurred_at < $cutoff".update.run())
        hashes <- ApiResponseBody.deleteOrphanRows
      } yield (count, hashes)
    }.flatMap { case (count, hashes) =>
      ZIO.foreachDiscard(hashes)(hash => BodyStore.delete(hash).ignore).as(count)
    }
}
