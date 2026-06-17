package ccas.cli

import java.nio.file.{Path, Paths}

/** Resolves the ccas XDG directories and the files within them: the completion cache (matching the exact paths the
  * generated completion scripts read, `${XDG_CACHE_HOME:-$HOME/.cache}/ccas/`) and the CLI config file
  * (`${XDG_CONFIG_HOME:-$HOME/.config}/ccas/config.conf`). Kept in lock-step with [[CompletionEmitter]]'s shell helpers.
  */
object XdgPaths {

  /** `$XDG_CACHE_HOME/ccas`, falling back to `$HOME/.cache/ccas` when `XDG_CACHE_HOME` is unset or blank. */
  def cacheDir: Path = xdgDir("XDG_CACHE_HOME", "/.cache")

  /** `$XDG_CONFIG_HOME/ccas`, falling back to `$HOME/.config/ccas` when `XDG_CONFIG_HOME` is unset or blank. */
  def configDir: Path = xdgDir("XDG_CONFIG_HOME", "/.config")

  def clubsFile: Path = cacheDir.resolve("clubs.txt")

  def recentJobsFile: Path = cacheDir.resolve("recent-jobs.txt")

  def configFile: Path = configDir.resolve("config.conf")

  private def xdgDir(envVar: String, homeRelative: String): Path = {
    val xdg = Option(System.getenv(envVar)).map(_.trim).filter(_.nonEmpty)
    val base = xdg.getOrElse(System.getProperty("user.home") + homeRelative)
    Paths.get(base, "ccas")
  }
}
