package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

/** Generic single-row-per-key store (`app_setting (key TEXT PK, value TEXT)`) for DB-owned app-level settings: values
  * tunable at runtime without a config-file edit or redeploy, and consistent across every process sharing one DB (the
  * server plus any CLI invocation that boots `Tables.ensureTables` and runs the cache sweep). A single server is the
  * supported deployment model (see CLAUDE.md "Deployment model"). Kept orthogonal to `client_config`, which holds
  * per-process `ChessComClient` tuning.
  *
  * Typed access goes through [[AppSettingKey]] / [[AppSettings]] so the stringly-typed table is confined to one place;
  * each key carries a compiled-in default that is the fallback when its row is absent (fresh DB) or unparseable. There
  * is no HOCON/env mirror — the DB row is the only tunable source, the code constant the only default.
  */
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class AppSetting(key: String, value: String) derives DbCodec

/** A typed app-setting: its DB key, compiled-in default, and the string <-> A codec used to (de)serialise the row. */
final case class AppSettingKey[A](key: String, default: A, parse: String => Option[A], render: A => String)

object AppSettings {

  /** Retention window (days) for `api_response_cache`, applied by `Tables.ensureTables`. */
  val CacheRetentionDays: AppSettingKey[Int] =
    AppSettingKey("cache_retention_days", 7, _.toIntOption, _.toString)

  /** Every known key — for discoverability (e.g. a future `ccas settings list`). */
  val all: List[AppSettingKey[?]] = List(CacheRetentionDays)
}

object AppSetting {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS app_setting (
              key   TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )""".update.run()
    }

  /** Typed read: the stored value parsed, falling back to the key's compiled default when the row is absent or
    * unparseable. An unparseable value is logged and treated as absent rather than failing startup.
    */
  def get[A](k: AppSettingKey[A]): ZIO[PostgresClient, SQLException, A] =
    selectRaw(k.key).flatMap {
      case None      => ZIO.succeed(k.default)
      case Some(raw) => parseOrDefault(k, raw)
    }

  /** Typed write: insert or overwrite the setting. Returns rows affected. */
  def set[A](k: AppSettingKey[A], value: A): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      val raw = k.render(value)
      sql"""INSERT INTO app_setting (key, value) VALUES (${k.key}, $raw)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[AppSetting]] =
    connectZIO {
      sql"SELECT key, value FROM app_setting".query[AppSetting].run().toList
    }

  private def parseOrDefault[A](k: AppSettingKey[A], raw: String): ZIO[Any, Nothing, A] =
    k.parse(raw) match {
      case Some(a) => ZIO.succeed(a)
      case None =>
        ZIO
          .logWarning(s"app_setting '${k.key}' = '$raw' is unparseable; using default ${k.render(k.default)}")
          .as(k.default)
    }

  private def selectRaw(key: String): ZIO[PostgresClient, SQLException, Option[String]] =
    connectZIO {
      sql"SELECT value FROM app_setting WHERE key = $key".query[String].run().headOption
    }
}
