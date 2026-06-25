package ccas.cli.config

import java.nio.file.Path

import zio.{Config, IO}
import zio.config.typesafe.TypesafeConfigProvider

/** User-facing CLI settings, read from `${XDG_CONFIG_HOME:-~/.config}/ccas/config.conf` (HOCON).
  *
  * This is the CLI *client* config and is deliberately separate from the server's classpath `application.conf` (DB,
  * port, Chess.com client) — hence [[load]] reads one specific external file via `fromHoconFileZIO`, never
  * `fromResourcePath`/`ConfigFactory.load()`, which would merge the server config and cross-wire the two.
  *
  * All keys are optional: a missing file (or a file missing a key) yields the corresponding empty value, so the CLI
  * always has working defaults. `default_clubs` feeds shell completions; `api_url` is the resolved server URL's
  * config-level default (overridden by `--server`, falling back to the built-in default). `log_dir` sets where a
  * detached `ccas serve --detach` server writes `server.log` (default `${XDG_STATE_HOME:-~/.local/state}/ccas/logs`).
  * `current_club` is the club a slug-requiring command targets when neither `--club` nor `--all` is given (set with
  * `ccas use-club <slug>`, written by [[ConfigWriter]]).
  */
final case class CliConfig(
  apiUrl: Option[String],
  defaultClubs: List[String],
  logDir: Option[String],
  currentClub: Option[String]
)

object CliConfig {

  val empty: CliConfig = CliConfig(None, Nil, None, None)

  private val descriptor: Config[CliConfig] =
    (Config.string("api_url").optional.map(blankToNone) ++
      Config.listOf("default_clubs", Config.string).withDefault(Nil) ++
      Config.string("log_dir").optional.map(blankToNone) ++
      Config.string("current_club").optional.map(blankToNone)).map {
      case (apiUrl, defaultClubs, logDir, currentClub) =>
        CliConfig(apiUrl, defaultClubs, logDir.map(expandTilde), currentClub)
    }

  // A present-but-blank value (`api_url = ""` / whitespace) is treated as unset, so it falls back to the next
  // resolution tier rather than resolving to an empty server URL that breaks every request.
  private def blankToNone(o: Option[String]): Option[String] = o.filter(_.trim.nonEmpty)

  /** Read the config at `file`, returning a ready-to-print error string on failure. `fromHoconFileZIO` parses the file
    * inside the ZIO (its underlying `ConfigFactory.parseFile` allows-missing, so a non-existent file becomes an empty
    * provider and — every key being optional — yields [[empty]]; the CLI never crashes for want of a config). A
    * malformed file (parse error, or a wrong-typed key from the descriptor) is collapsed to one
    * `invalid config file <path>: …` message so the caller just prints it.
    */
  def load(file: Path): IO[String, CliConfig] =
    TypesafeConfigProvider
      .fromHoconFileZIO(file.toFile)
      .flatMap(_.load(descriptor))
      .mapError(e => s"invalid config file $file: ${rootMessage(e)}")

  private def rootMessage(e: Throwable): String = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)

  // HOCON has no `~` expansion; do it ourselves so `log_dir = "~/.local/state/ccas/logs"` resolves to an absolute path.
  private def expandTilde(p: String): String = {
    val home = System.getProperty("user.home")
    if (p == "~") { home }
    else if (p.startsWith("~/")) { home + p.drop(1) }
    else { p }
  }
}
