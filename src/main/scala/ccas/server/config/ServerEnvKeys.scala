package ccas.server.config

/** Registry of the environment variables the `ccas` / `ccas-server` binaries read at boot (resolved by
  * `application.conf` via `${?VAR}` substitution), the open-ended set `ccas config` knows how to describe. Mirrors
  * `ccas.analysis.tables.AppSetting.Key`'s "confine the stringly-typed names to one place" intent.
  *
  * `domain` groups keys for display — `ccas config list` prints a `# <domain>` header per group. `Secret` keys are
  * redacted by `list`/`show` unless `--show-secrets`. `values` is what shell completion offers after
  * `ccas config set <key>`. The registry is descriptive, not exhaustive: `ccas config set` accepts any key and any
  * value (env vars are open-ended) and only *warns* on a key not listed here, or on a value its key doesn't take.
  */
final case class ServerEnvKey(
  name: String,
  description: String,
  domain: ServerEnvKey.Domain,
  sensitivity: ServerEnvKey.Sensitivity,
  values: ServerEnvKey.Values
)

object ServerEnvKey {
  enum Domain(val label: String) {
    case Contact        extends Domain("Contact")
    case Database       extends Domain("Database")
    case Server         extends Domain("Server")
    case Scheduler      extends Domain("Scheduler")
    case ChessComClient extends Domain("Chess.com client")
    case BodyStore      extends Domain("Response-body store")
  }

  enum Sensitivity {
    case Plain, Secret
  }

  enum Values {
    case Free
    // Read with Typesafe Config's getBoolean, which also takes yes/on and no/off: accepted, but not offered.
    case Toggle
    case OneOf(choices: List[String])

    /** What completion offers, in order. */
    def candidates: List[String] = this match {
      case Free           => Nil
      case Toggle         => List("true", "false")
      case OneOf(choices) => choices
    }

    /** False when the server would refuse `value` for this key; a free-form key's values aren't checked. */
    def accepts(value: String): Boolean = this match {
      case Free           => true
      case Toggle         => toggleWords.contains(value)
      case OneOf(choices) => choices.contains(value)
    }
  }

  private val toggleWords = Set("true", "yes", "on", "false", "no", "off")
}

object ServerEnvKeys {

  import ServerEnvKey.Domain.*
  import ServerEnvKey.Sensitivity.*
  import ServerEnvKey.Values.*

  // One row per key: scalafmt would wrap the longest rows, which run past 120 columns.
  // format: off
  val all: List[ServerEnvKey] = List(
    ServerEnvKey("CCAS_CONTACT_EMAIL", "Contact email for the Chess.com API User-Agent (required)", Contact, Plain, Free),
    ServerEnvKey("DATABASE_URL", "Full JDBC URL; takes priority over the DB_* fields", Database, Secret, Free),
    ServerEnvKey("DB_HOST", "Database host (used only when DATABASE_URL is absent)", Database, Plain, Free),
    ServerEnvKey("DB_PORT", "Database port", Database, Plain, Free),
    ServerEnvKey("DB_NAME", "Database name", Database, Plain, Free),
    ServerEnvKey("DB_USER", "Database user", Database, Plain, Free),
    ServerEnvKey("DB_PASSWORD", "Database password", Database, Secret, Free),
    ServerEnvKey("DB_SCHEMA", "Database schema (search_path)", Database, Plain, Free),
    ServerEnvKey("SERVER_PORT", "HTTP server port (default 8080)", Server, Plain, Free),
    ServerEnvKey("SERVER_HOST", "Bind address (default 127.0.0.1; 0.0.0.0 for hosted)", Server, Plain, Free),
    ServerEnvKey("JOB_LOGS_DIR", "Directory for per-job log files", Server, Plain, Free),
    ServerEnvKey("CCAS_OUTPUT_DIR", "Directory for app reports", Server, Plain, Free),
    ServerEnvKey("SCHEDULER_POLL_MINUTES", "Scheduler poll interval (default 15)", Scheduler, Plain, Free),
    ServerEnvKey("SCHEDULER_MATCHREF_INTERVAL_HOURS", "MatchRef maintenance cadence (default 24)", Scheduler, Plain, Free),
    ServerEnvKey("SCHEDULER_MATCHREF_ENABLED", "Seed MatchRef schedule enabled (default true)", Scheduler, Plain, Toggle),
    ServerEnvKey("SCHEDULER_CLUBDATA_INTERVAL_HOURS", "ClubData refresh cadence (default 6)", Scheduler, Plain, Free),
    ServerEnvKey("SCHEDULER_CLUBDATA_ENABLED", "Seed ClubData schedule enabled (default true)", Scheduler, Plain, Toggle),
    ServerEnvKey("SCHEDULER_HISTORY_INTERVAL_HOURS", "Per-club History cadence (default 24)", Scheduler, Plain, Free),
    ServerEnvKey("SCHEDULER_HISTORY_ENABLED", "Seed History schedule enabled (default true)", Scheduler, Plain, Toggle),
    ServerEnvKey("SCHEDULER_MEMBERSHIP_INTERVAL_HOURS", "Per-club Membership cadence (default 24)", Scheduler, Plain, Free),
    ServerEnvKey("SCHEDULER_MEMBERSHIP_ENABLED", "Seed Membership schedule enabled (default true)", Scheduler, Plain, Toggle),
    ServerEnvKey("DB_POOL_MAX", "HikariCP max pool size (default 20)", Database, Plain, Free),
    ServerEnvKey("DB_POOL_MIN_IDLE", "HikariCP min idle (default 2; 0 for Neon scale-to-zero)", Database, Plain, Free),
    ServerEnvKey("DB_POOL_CONNECTION_TIMEOUT", "HikariCP connectionTimeout ms (default 30000)", Database, Plain, Free),
    ServerEnvKey("DB_POOL_IDLE_TIMEOUT", "HikariCP idleTimeout ms (default 600000)", Database, Plain, Free),
    ServerEnvKey("DB_POOL_MAX_LIFETIME", "HikariCP maxLifetime ms (default 1800000)", Database, Plain, Free),
    ServerEnvKey("DB_POOL_KEEPALIVE_TIME", "HikariCP keepaliveTime ms (default 120000)", Database, Plain, Free),
    ServerEnvKey("DB_POOL_CONNECTION_TEST_QUERY", "HikariCP connectionTestQuery (default SELECT 1)", Database, Plain, Free),
    ServerEnvKey("DB_POOL_INIT_FAIL_TIMEOUT", "HikariCP initializationFailTimeout (default -1)", Database, Plain, Free),
    ServerEnvKey("DB_SOCKET_TIMEOUT_SECONDS", "pgjdbc socket read timeout s (default 60)", Database, Plain, Free),
    ServerEnvKey("DB_CONNECT_TIMEOUT_SECONDS", "pgjdbc connect timeout s (default 10)", Database, Plain, Free),
    ServerEnvKey("DB_TCP_KEEP_ALIVE", "pgjdbc SO_KEEPALIVE (default true)", Database, Plain, Toggle),
    ServerEnvKey("DB_RETRY_BASE_DELAY_MS", "Transient-error retry base delay ms (default 100)", Database, Plain, Free),
    ServerEnvKey("DB_RETRY_MAX_RETRIES", "Transient-error retry count (default 3)", Database, Plain, Free),
    ServerEnvKey("CHESS_COM_API_RECOVERY_TIERS", "Throttle recovery tiers (default [2,4,6,8])", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_COOLDOWN_SECONDS", "Backoff cooldown after 429 (default 15)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_CF_COOLDOWN_SECONDS", "Cloudflare-403 cooldown (default 30)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_FAILURE_WINDOW_SIZE", "Failure-window size (default 40)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_FAILURE_THRESHOLD", "Failure-window throttle threshold (default 0.2)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_MIN_SAMPLE_SIZE", "Min samples before throttling (default 15)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_MIN_REQUEST_DELAY_MS", "Min request-spacing floor ms (default 75)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_EMA_TAU_MS", "EMA spacing time constant ms (default 500)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_MIN_TIER_OBSERVATION_SECONDS", "Min tier observation s (default 10)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_RETRY_BASE_SECONDS", "429 retry base s (default 1)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_CF_RETRY_DELAY_SECONDS", "Cloudflare-403 retry delay s (default 10)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_CONNECTION_RETRY_BASE_SECONDS", "Connection-error retry base s (default 1)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_MAX_429_RETRIES", "Max 429 retries (default 5)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_MAX_CF_RETRIES", "Max Cloudflare-403 retries (default 2)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_MAX_CONNECTION_RETRIES", "Max connection-error retries (default 3)", ChessComClient, Plain, Free),
    ServerEnvKey("CHESS_COM_API_STATS_FLUSH_INTERVAL_SECONDS", "Client-stats flush interval s (default 30)", ChessComClient, Plain, Free),
    // Response-body store (#191): where cached Chess.com bodies live. The s3-* keys are only consulted when backend = s3.
    // The backend's choices are the cases of `BodyStore.build`'s match, kept in step by hand.
    ServerEnvKey("CCAS_BODY_STORE_BACKEND", "Response-body store backend: fs (default) or s3", BodyStore, Plain, OneOf(List("fs", "s3"))),
    ServerEnvKey("CCAS_BODY_STORE_FS_ROOT", "Filesystem root for the fs body store", BodyStore, Plain, Free),
    ServerEnvKey("CCAS_R2_ENDPOINT", "R2 S3 endpoint URL (when backend = s3)", BodyStore, Plain, Free),
    ServerEnvKey("CCAS_R2_BUCKET", "R2 bucket name (when backend = s3)", BodyStore, Plain, Free),
    ServerEnvKey("CCAS_R2_REGION", "R2 region (default auto)", BodyStore, Plain, Free),
    ServerEnvKey("CCAS_R2_ACCESS_KEY", "R2 S3 access key id (when backend = s3)", BodyStore, Secret, Free),
    ServerEnvKey("CCAS_R2_SECRET_KEY", "R2 S3 secret access key (when backend = s3)", BodyStore, Secret, Free),
    ServerEnvKey("CCAS_BODY_STORE_READ_TIMEOUT_MS", "Body-store read deadline ms (default 5000)", BodyStore, Plain, Free),
    ServerEnvKey("CCAS_BODY_STORE_WRITE_TIMEOUT_MS", "Body-store write deadline ms (default 10000)", BodyStore, Plain, Free),
    ServerEnvKey("CCAS_R2_CONNECT_TIMEOUT_MS", "R2 transport connect timeout ms (default 2000)", BodyStore, Plain, Free),
    ServerEnvKey("CCAS_R2_SOCKET_TIMEOUT_MS", "R2 transport socket timeout ms (default 5000)", BodyStore, Plain, Free)
  )
  // format: on

  /** Keys grouped by display [[ServerEnvKey.Domain]], in enum-declaration order, each group in registry order. Empty
    * domains are dropped. Drives the grouped `ccas config list` output; iterates `Domain.values` (stable) rather than
    * `all.groupBy` (Map order is unstable).
    */
  def grouped: List[(ServerEnvKey.Domain, List[ServerEnvKey])] =
    ServerEnvKey.Domain.values.toList
      .map(d => d -> all.filter(_.domain == d))
      .filter { case (_, ks) => ks.nonEmpty }

  def byName(name: String): Option[ServerEnvKey] = all.find(_.name == name)

  def isSecret(name: String): Boolean = byName(name).exists(_.sensitivity == Secret)

  /** Mask a secret value for display; pass non-secret values through unchanged. A blank value is shown verbatim. */
  def redact(name: String, value: String): String =
    if (isSecret(name) && value.nonEmpty) { "****" }
    else { value }
}
