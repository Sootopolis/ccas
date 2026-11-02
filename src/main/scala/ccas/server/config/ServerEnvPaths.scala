package ccas.server.config

import java.nio.file.Path

import ccas.utils.AppDir.XdgBase

/** Where the server-bootstrap env file lives (`${XDG_CONFIG_HOME:-~/.config}/ccas/ccas.env`), for `CcasServer` (the
  * standalone `ccas-server` entry point) to apply [[ServerEnvOverlay]] at boot and for `ccas.cli.CliWiring`, keeping one
  * source of truth without a server→cli package cycle.
  */
object ServerEnvPaths {

  /** The file in `configDir`, the directory [[XdgBase.Config]] resolves to. */
  def in(configDir: Path): Path = configDir.resolve("ccas.env")

  /** The file on this machine, or why there is nowhere to look for it. */
  def file: Either[String, Path] =
    XdgBase.Config.ccasDirHere.map(in).left.map(reason => s"could not look for server config ccas.env: $reason")
}
