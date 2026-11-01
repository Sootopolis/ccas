package ccas.cli

import java.nio.file.Path

import zio.ZLayer
import zio.http.Client

import ccas.server.config.ServerEnvPaths
import ccas.utils.client.HttpClientLayer

/** What the commands reach outside the process with: the config file holding `current_club`, the completion cache's
  * club list and recent job ids, the server-bootstrap env file that `ccas config` manages and the `server` commands
  * read, and the client the server-backed commands ask the server with. [[Main]] builds it once and hands it down,
  * because those paths come from environment variables a running JVM cannot rebind — so a test hands it a temp
  * directory and a fake server instead.
  */
private[cli] final case class CliWiring(
  configFile: Path,
  clubsFile: Path,
  recentJobsFile: Path,
  serverEnvFile: Path,
  clientLayer: ZLayer[Any, Throwable, Client]
)

private[cli] object CliWiring {

  /** The developer's own files. Only this resolves them as paths, so code handed a wiring has no shortcut to them; the
    * cache's names match those [[CompletionEmitter]]'s scripts read, and the env file is the one the standalone server
    * reads too ([[ServerEnvPaths]]).
    */
  def live: CliWiring =
    CliWiring(
      configFile = XdgPaths.configDir.resolve("config.conf"),
      clubsFile = XdgPaths.cacheDir.resolve("clubs.txt"),
      recentJobsFile = XdgPaths.cacheDir.resolve("recent-jobs.txt"),
      serverEnvFile = ServerEnvPaths.file,
      clientLayer = liveClient
    )

  // `HttpClientLayer.live`, not `Client.default`, for its 10s connect timeout (#182): a black-holed server fails in
  // Netty, below the uninterruptible unwind a ZIO `.timeout` would hit, leaving slow calls and streaming follows alone.
  // Suspended so that, as before #296, only a command that asks a server initialises `HttpClientLayer`.
  private val liveClient: ZLayer[Any, Throwable, Client] = ZLayer.suspend(HttpClientLayer.live)
}
