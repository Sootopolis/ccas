package ccas.server.config

import java.nio.file.{Path, Paths}

/** Resolves the server-bootstrap env file path (`${XDG_CONFIG_HOME:-~/.config}/ccas/ccas.env`) without depending on the
  * `ccas.cli` package, so `CcasServer` (the standalone `ccas-server` entry point) can apply [[ServerEnvOverlay]] at boot
  * without a server→cli package cycle. `ccas.cli.XdgPaths.serverEnvFile` delegates here, keeping one source of truth and
  * the same XDG resolution as the CLI's other paths.
  */
object ServerEnvPaths {

  def file: Path = {
    val xdg  = Option(System.getenv("XDG_CONFIG_HOME")).map(_.trim).filter(_.nonEmpty)
    val base = xdg.getOrElse(System.getProperty("user.home") + "/.config")
    Paths.get(base, "ccas", "ccas.env")
  }
}
