package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class ClientConfig(
  configId: Long,
  permits: Int,
  cooldownSecs: Int,
  cfCooldownSecs: Int,
  retryBaseSecs: Int,
  singleRetrySecs: Int,
  cfRetrySecs: Int,
  failureWindowSize: Int,
  failureThreshold: Double,
  minSampleSize: Int
) derives DbCodec

object ClientConfig {

  private val selectCols = SqlLiteral(
    """config_id, permits, cooldown_secs, cf_cooldown_secs,
       retry_base_secs, single_retry_secs, cf_retry_secs,
       failure_window_size, failure_threshold, min_sample_size"""
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS client_config (
              config_id            BIGSERIAL PRIMARY KEY,
              permits              INT NOT NULL,
              cooldown_secs        INT NOT NULL,
              cf_cooldown_secs     INT NOT NULL,
              retry_base_secs      INT NOT NULL,
              single_retry_secs    INT NOT NULL,
              cf_retry_secs        INT NOT NULL,
              failure_window_size  INT NOT NULL,
              failure_threshold    DOUBLE PRECISION NOT NULL,
              min_sample_size      INT NOT NULL
            )""".update.run()
    }

  def selectId(configId: Long): ZIO[PostgresClient, SQLException, Option[ClientConfig]] =
    connectZIO {
      sql"SELECT $selectCols FROM client_config WHERE config_id = $configId"
        .query[ClientConfig].run().headOption
    }

  def insert(item: ClientConfig): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO client_config (
              permits, cooldown_secs, cf_cooldown_secs,
              retry_base_secs, single_retry_secs, cf_retry_secs,
              failure_window_size, failure_threshold, min_sample_size
            ) VALUES (
              ${item.permits}, ${item.cooldownSecs}, ${item.cfCooldownSecs},
              ${item.retryBaseSecs}, ${item.singleRetrySecs}, ${item.cfRetrySecs},
              ${item.failureWindowSize}, ${item.failureThreshold}, ${item.minSampleSize}
            ) RETURNING config_id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def deleteAll: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM client_config".update.run()
    }
}
