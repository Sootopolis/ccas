package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.{UIO, ZIO}

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

/** Generic single-row-per-key store (`app_setting (key TEXT PK, value TEXT)`) for DB-owned app-level settings: values
  * tunable at runtime without a config-file edit or redeploy, and consistent across every process sharing one DB (the
  * server plus any CLI invocation that boots `Tables.ensureTables`). A single server is the supported deployment model
  * (see CLAUDE.md "Deployment model"). Kept orthogonal to `client_config`, which holds per-process `ChessComClient`
  * tuning.
  *
  * Typed access goes through [[AppSetting.Key]] and the registry in the companion (`CacheRetentionDays`, `all`) so the
  * stringly-typed table is confined to one place; each key carries a compiled-in default, the fallback when its row is
  * absent (fresh DB) or unparseable. There is no HOCON/env mirror — the DB row is the only tunable source, the code
  * constant the only default.
  */
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class AppSetting(key: String, value: String) derives DbCodec

object AppSetting {

  /** A typed setting: its DB key, compiled-in default, and the string <-> A codec used to (de)serialise the row. */
  final case class Key[A](key: String, default: A, parse: String => Option[A], render: A => String)

  /** Retention window (days) for `api_response_cache`, applied by the server's retention sweep. */
  // 60 rather than the original 7: bodies left metered Postgres for the BodyStore (#191), so the cost of keeping an
  // entry is R2 storage plus a ~200-byte metadata row. `touch` bumps `fetched_at` on every revalidation, so this only
  // decides how long an entry nothing revisits survives — 60 days covers a monthly crawl cycle with room for drift.
  val CacheRetentionDays: Key[Int] = Key("cache_retention_days", 60, _.toIntOption, _.toString)

  /** Retention window (days) for `api_fetch_failure`, applied alongside the cache sweep. Every failed attempt writes
    * a row (plus a deduped body), a 404 body embeds the requested slug so SHA-256 dedup never collapses distinct bogus
    * slugs, and orphan bodies are pinned by `ON DELETE RESTRICT` — so without this sweep the table grows unbounded
    * under any volume of bogus-slug traffic. These rows are the diagnostic audit trail for API failures, kept for
    * post-hoc analysis rather than to serve reads.
    */
  val FetchFailureRetentionDays: Key[Int] = Key("fetch_failure_retention_days", 30, _.toIntOption, _.toString)

  /** Retention window (days) for per-job log files in `${JOB_LOGS_DIR}`, applied by the server's retention sweep.
    * Must be positive — a non-positive value is rejected in favour of this default, so it cannot wipe the directory
    * on every pass.
    */
  val JobLogRetentionDays: Key[Int] = Key("job_log_retention_days", 14, _.toIntOption, _.toString)

  /** Sample spacing (milliseconds) for progress-bar frames on `GET /api/jobs/{id}/progress`. The server samples the
    * merged bar state at this interval and de-duplicates, so a busy job's updates collapse to at most one encoded +
    * sent frame per interval (an idle job sends none). Caps server work + follower traffic while still delivering the
    * latest state within one interval. Default 100ms (10 fps): smooth for a bar, light on the server; floored at 16ms.
    * Read per subscribe in `JobRunner`.
    */
  val ProgressRefreshIntervalMillis: Key[Int] = Key("progress_refresh_interval_ms", 100, _.toIntOption, _.toString)

  /** Every known key — for discoverability (e.g. a future `ccas settings list`). */
  val all: List[Key[?]] =
    List(CacheRetentionDays, FetchFailureRetentionDays, JobLogRetentionDays, ProgressRefreshIntervalMillis)

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
  def get[A](k: Key[A]): ZIO[PostgresClient, SQLException, A] =
    selectRaw(k.key).flatMap {
      case None      => ZIO.succeed(k.default)
      case Some(raw) => parseOrDefault(k, raw)
    }

  /** Typed write: insert or overwrite the setting. Returns rows affected. */
  def set[A](k: Key[A], value: A): ZIO[PostgresClient, SQLException, Int] = {
    val raw = k.render(value)
    connectZIO {
      sql"""INSERT INTO app_setting (key, value) VALUES (${k.key}, $raw)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""".update.run()
    }
  }

  def selectAll: ZIO[PostgresClient, SQLException, List[AppSetting]] =
    connectZIO {
      sql"SELECT key, value FROM app_setting".query[AppSetting].run().toList
    }

  private def parseOrDefault[A](k: Key[A], raw: String): UIO[A] =
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
