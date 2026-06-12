package ccas.cli

import java.nio.file.{Path, Paths}

/** Resolves the ccas cache directory and completion cache files, matching the exact paths the generated completion
  * scripts read: `${XDG_CACHE_HOME:-$HOME/.cache}/ccas/`. Kept in lock-step with [[CompletionEmitter]]'s shell helpers.
  */
object XdgPaths {

  /** `$XDG_CACHE_HOME/ccas`, falling back to `$HOME/.cache/ccas` when `XDG_CACHE_HOME` is unset or blank. */
  def cacheDir: Path = {
    val xdg = Option(System.getenv("XDG_CACHE_HOME")).map(_.trim).filter(_.nonEmpty)
    val base = xdg.getOrElse(System.getProperty("user.home") + "/.cache")
    Paths.get(base, "ccas")
  }

  def clubsFile: Path = cacheDir.resolve("clubs.txt")

  def recentJobsFile: Path = cacheDir.resolve("recent-jobs.txt")
}
