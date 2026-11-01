package ccas.cli

import java.nio.file.{Path, Paths}

/** Resolves the ccas XDG directories: the cache (matching the exact paths the generated completion scripts read,
  * `${XDG_CACHE_HOME:-$HOME/.cache}/ccas/`), the config directory (`${XDG_CONFIG_HOME:-$HOME/.config}/ccas/`), and the
  * runtime state dir holding the pid files of `ccas server up --detach` / `ccas server down`
  * (`${XDG_STATE_HOME:-$HOME/.local/state}/ccas/`). The cache and config files commands use are resolved in
  * [[CliWiring.live]] instead, so that nothing handed a wiring has a shortcut to them. Kept in lock-step with
  * [[CompletionEmitter]]'s shell helpers.
  */
object XdgPaths {

  /** `$XDG_CACHE_HOME/ccas`, falling back to `$HOME/.cache/ccas` when `XDG_CACHE_HOME` is unset or blank. */
  def cacheDir: Path = xdgDir("XDG_CACHE_HOME", "/.cache")

  /** `$XDG_CONFIG_HOME/ccas`, falling back to `$HOME/.config/ccas` when `XDG_CONFIG_HOME` is unset or blank. */
  def configDir: Path = xdgDir("XDG_CONFIG_HOME", "/.config")

  /** `$XDG_STATE_HOME/ccas`, falling back to `$HOME/.local/state/ccas` when `XDG_STATE_HOME` is unset or blank. */
  def stateDir: Path = xdgDir("XDG_STATE_HOME", "/.local/state")

  private def xdgDir(envVar: String, homeRelative: String): Path = {
    val xdg  = Option(System.getenv(envVar)).map(_.trim).filter(_.nonEmpty)
    val base = xdg.getOrElse(System.getProperty("user.home") + homeRelative)
    Paths.get(base, "ccas")
  }
}
