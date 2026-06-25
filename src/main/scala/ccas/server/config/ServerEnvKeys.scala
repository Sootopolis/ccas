package ccas.server.config

/** Registry of the environment variables the `ccas` / `ccas-server` binaries read at boot (resolved by
  * `application.conf` via `${?VAR}` substitution), the open-ended set `ccas config` knows how to describe. Mirrors
  * `ccas.analysis.tables.AppSetting.Key`'s "confine the stringly-typed names to one place" intent.
  *
  * `Essential` keys are what `ccas config init` prompts for and `Main.missingServeEnv` checks (the server can't boot
  * without them); `Optional` keys are documented overrides shown by `ccas config list`. `secret` keys (DATABASE_URL,
  * DB_PASSWORD) are redacted by `list`/`show` unless `--show-secrets`. The registry is descriptive, not exhaustive:
  * `ccas config set` accepts any key (env vars are open-ended) and only *warns* on one not listed here.
  */
final case class ServerEnvKey(name: String, description: String, secret: Boolean, group: ServerEnvKey.Group)

object ServerEnvKey {
  enum Group {
    case Essential, Optional
  }
}

object ServerEnvKeys {

  import ServerEnvKey.Group.*

  val all: List[ServerEnvKey] = List(
    // Essential — prompted by `init`, checked by `missingServeEnv`.
    ServerEnvKey("CCAS_CONTACT_EMAIL", "Contact email for the Chess.com API User-Agent (required)", secret = false, Essential),
    ServerEnvKey("DATABASE_URL", "Full JDBC URL; takes priority over the DB_* fields", secret = true, Essential),
    ServerEnvKey("DB_HOST", "Database host (used only when DATABASE_URL is absent)", secret = false, Essential),
    ServerEnvKey("DB_PORT", "Database port", secret = false, Essential),
    ServerEnvKey("DB_NAME", "Database name", secret = false, Essential),
    ServerEnvKey("DB_USER", "Database user", secret = false, Essential),
    ServerEnvKey("DB_PASSWORD", "Database password", secret = true, Essential),
    ServerEnvKey("DB_SCHEMA", "Database schema (search_path)", secret = false, Essential),
    // Optional — documented overrides, shown by `list`, not prompted.
    ServerEnvKey("SERVER_PORT", "HTTP server port (default 8080)", secret = false, Optional),
    ServerEnvKey("SERVER_HOST", "Bind address (default 127.0.0.1; 0.0.0.0 for hosted)", secret = false, Optional),
    ServerEnvKey("JOB_LOGS_DIR", "Directory for per-job log files", secret = false, Optional),
    ServerEnvKey("SCHEDULER_POLL_MINUTES", "Scheduler poll interval (default 15)", secret = false, Optional),
    ServerEnvKey("SCHEDULER_MATCHREF_INTERVAL_HOURS", "MatchRef maintenance cadence (default 24)", secret = false, Optional),
    ServerEnvKey("SCHEDULER_MATCHREF_ENABLED", "Seed MatchRef schedule enabled (default true)", secret = false, Optional),
    ServerEnvKey("SCHEDULER_CLUBDATA_INTERVAL_HOURS", "ClubData refresh cadence (default 6)", secret = false, Optional),
    ServerEnvKey("SCHEDULER_CLUBDATA_ENABLED", "Seed ClubData schedule enabled (default true)", secret = false, Optional),
    ServerEnvKey("SCHEDULER_HISTORY_INTERVAL_HOURS", "Per-club History cadence (default 24)", secret = false, Optional),
    ServerEnvKey("SCHEDULER_HISTORY_ENABLED", "Seed History schedule enabled (default true)", secret = false, Optional),
    ServerEnvKey("SCHEDULER_MEMBERSHIP_INTERVAL_HOURS", "Per-club Membership cadence (default 24)", secret = false, Optional),
    ServerEnvKey("SCHEDULER_MEMBERSHIP_ENABLED", "Seed Membership schedule enabled (default true)", secret = false, Optional),
    ServerEnvKey("DB_POOL_MAX", "HikariCP max pool size (default 20)", secret = false, Optional),
    ServerEnvKey("DB_POOL_MIN_IDLE", "HikariCP min idle (default 2; 0 for Neon scale-to-zero)", secret = false, Optional),
    ServerEnvKey("DB_POOL_CONNECTION_TIMEOUT", "HikariCP connectionTimeout ms (default 30000)", secret = false, Optional),
    ServerEnvKey("DB_POOL_IDLE_TIMEOUT", "HikariCP idleTimeout ms (default 600000)", secret = false, Optional),
    ServerEnvKey("DB_POOL_MAX_LIFETIME", "HikariCP maxLifetime ms (default 1800000)", secret = false, Optional),
    ServerEnvKey("DB_POOL_KEEPALIVE_TIME", "HikariCP keepaliveTime ms (default 120000)", secret = false, Optional),
    ServerEnvKey("DB_POOL_CONNECTION_TEST_QUERY", "HikariCP connectionTestQuery (default SELECT 1)", secret = false, Optional),
    ServerEnvKey("DB_POOL_INIT_FAIL_TIMEOUT", "HikariCP initializationFailTimeout (default -1)", secret = false, Optional),
    ServerEnvKey("DB_SOCKET_TIMEOUT_SECONDS", "pgjdbc socket read timeout s (default 60)", secret = false, Optional),
    ServerEnvKey("DB_CONNECT_TIMEOUT_SECONDS", "pgjdbc connect timeout s (default 10)", secret = false, Optional),
    ServerEnvKey("DB_TCP_KEEP_ALIVE", "pgjdbc SO_KEEPALIVE (default true)", secret = false, Optional),
    ServerEnvKey("DB_RETRY_BASE_DELAY_MS", "Transient-error retry base delay ms (default 100)", secret = false, Optional),
    ServerEnvKey("DB_RETRY_MAX_RETRIES", "Transient-error retry count (default 3)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_RECOVERY_TIERS", "Throttle recovery tiers (default [2,4,6,8])", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_COOLDOWN_SECONDS", "Backoff cooldown after 429 (default 15)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_CF_COOLDOWN_SECONDS", "Cloudflare-403 cooldown (default 30)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_FAILURE_WINDOW_SIZE", "Failure-window size (default 40)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_FAILURE_THRESHOLD", "Failure-window throttle threshold (default 0.2)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_MIN_SAMPLE_SIZE", "Min samples before throttling (default 15)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_MIN_REQUEST_DELAY_MS", "Min request-spacing floor ms (default 75)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_EMA_TAU_MS", "EMA spacing time constant ms (default 500)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_MIN_TIER_OBSERVATION_SECONDS", "Min tier observation s (default 10)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_RETRY_BASE_SECONDS", "429 retry base s (default 1)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_CF_RETRY_DELAY_SECONDS", "Cloudflare-403 retry delay s (default 10)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_CONNECTION_RETRY_BASE_SECONDS", "Connection-error retry base s (default 1)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_MAX_429_RETRIES", "Max 429 retries (default 5)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_MAX_CF_RETRIES", "Max Cloudflare-403 retries (default 2)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_MAX_CONNECTION_RETRIES", "Max connection-error retries (default 3)", secret = false, Optional),
    ServerEnvKey("CHESS_COM_API_STATS_FLUSH_INTERVAL_SECONDS", "Client-stats flush interval s (default 30)", secret = false, Optional)
  )

  val essential: List[ServerEnvKey] = all.filter(_.group == Essential)

  def byName(name: String): Option[ServerEnvKey] = all.find(_.name == name)

  def isSecret(name: String): Boolean = byName(name).exists(_.secret)

  /** Mask a secret value for display; pass non-secret values through unchanged. A blank value is shown verbatim. */
  def redact(name: String, value: String): String =
    if (isSecret(name) && value.nonEmpty) { "****" } else { value }
}
