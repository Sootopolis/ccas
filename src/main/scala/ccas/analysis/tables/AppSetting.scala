package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

/** Generic single-row-per-key store for app-level settings that must be consistent across consumers (e.g. multiple
  * CCAS machines on one Neon DB). Kept orthogonal to `client_config` (which holds `ChessComClient` tuning): this is
  * for app-wide policy. First consumer is `cache_retention_days`; future cross-consumer settings reuse the same shape
  * with no schema churn.
  */
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class AppSetting(key: String, value: String) derives DbCodec

object AppSetting {

  /** Retention window (days) for `api_response_cache`; read by `Tables.ensureTables`. */
  val CacheRetentionDays = "cache_retention_days"

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS app_setting (
              key   TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )""".update.run()
    }

  def select(key: String): ZIO[PostgresClient, SQLException, Option[String]] =
    connectZIO {
      sql"SELECT value FROM app_setting WHERE key = $key".query[String].run().headOption
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[AppSetting]] =
    connectZIO {
      sql"SELECT key, value FROM app_setting".query[AppSetting].run().toList
    }

  /** Idempotent seed: insert only when the key is absent, leaving any existing value untouched. */
  def insertIfAbsent(key: String, value: String): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO app_setting (key, value) VALUES ($key, $value)
            ON CONFLICT (key) DO NOTHING""".update.run()
    }

  /** Set (insert or overwrite) a setting; for admin writes that intentionally clobber the stored value. */
  def upsert(key: String, value: String): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO app_setting (key, value) VALUES ($key, $value)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""".update.run()
    }
}
