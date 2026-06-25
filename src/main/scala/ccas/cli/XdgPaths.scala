package ccas.cli

import java.nio.file.{Path, Paths}

/** Resolves the ccas XDG directories and the files within them: the completion cache (matching the exact paths the
  * generated completion scripts read, `${XDG_CACHE_HOME:-$HOME/.cache}/ccas/`), the CLI config file
  * (`${XDG_CONFIG_HOME:-$HOME/.config}/ccas/config.conf`), the server-bootstrap env file (`.../ccas/ccas.env`,
  * managed by `ccas config`), and the runtime state dir / pid file used by
  * `ccas serve --detach` / `ccas stop` (`${XDG_STATE_HOME:-$HOME/.local/state}/ccas/ccas.pid`). Kept in lock-step with
  * [[CompletionEmitter]]'s shell helpers.
  */
object XdgPaths {

  /** `$XDG_CACHE_HOME/ccas`, falling back to `$HOME/.cache/ccas` when `XDG_CACHE_HOME` is unset or blank. */
  def cacheDir: Path = xdgDir("XDG_CACHE_HOME", "/.cache")

  /** `$XDG_CONFIG_HOME/ccas`, falling back to `$HOME/.config/ccas` when `XDG_CONFIG_HOME` is unset or blank. */
  def configDir: Path = xdgDir("XDG_CONFIG_HOME", "/.config")

  /** `$XDG_STATE_HOME/ccas`, falling back to `$HOME/.local/state/ccas` when `XDG_STATE_HOME` is unset or blank. */
  def stateDir: Path = xdgDir("XDG_STATE_HOME", "/.local/state")

  def clubsFile: Path = cacheDir.resolve("clubs.txt")

  def recentJobsFile: Path = cacheDir.resolve("recent-jobs.txt")

  def configFile: Path = configDir.resolve("config.conf")

  /** Server-bootstrap env file (`KEY=VALUE`), managed by `ccas config` and applied at boot by `ServerEnvOverlay`.
    * Separate from [[configFile]]: that holds CLI *client* settings, this holds what the *server* needs to boot.
    * Delegates to `ServerEnvPaths` (in `ccas.server.config`) so the server can resolve the same path without a
    * server→cli package cycle — this accessor is the discoverable CLI-side alias.
    */
  def serverEnvFile: Path = ccas.server.config.ServerEnvPaths.file

  /** Pid file written by a detached `ccas serve --detach` server and read by `ccas stop`. */
  def pidFile: Path = stateDir.resolve("ccas.pid")

  private def xdgDir(envVar: String, homeRelative: String): Path = {
    val xdg = Option(System.getenv(envVar)).map(_.trim).filter(_.nonEmpty)
    val base = xdg.getOrElse(System.getProperty("user.home") + homeRelative)
    Paths.get(base, "ccas")
  }
}
