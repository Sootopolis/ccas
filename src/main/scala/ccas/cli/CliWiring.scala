package ccas.cli

import java.nio.file.Path

import zio.ZLayer
import zio.http.Client

import ccas.utils.client.HttpClientLayer

/** What the server-backed commands and `use-club` reach outside the process with: the config file holding
  * `current_club`, the completion cache's club list and recent job ids, and the client they ask the server with.
  * [[Main]] builds it once and hands it down, because [[XdgPaths]] reads environment variables a running JVM cannot
  * rebind — so a test hands it a temp directory and a fake server instead.
  */
private[cli] final case class CliWiring(
  configFile: Path,
  clubsFile: Path,
  recentJobsFile: Path,
  clientLayer: ZLayer[Any, Throwable, Client]
)

private[cli] object CliWiring {

  /** The developer's own files. Only this resolves them as paths, so code handed a wiring has no shortcut to them; the
    * cache's names match those [[CompletionEmitter]]'s scripts read.
    */
  def live: CliWiring =
    CliWiring(
      configFile = XdgPaths.configDir.resolve("config.conf"),
      clubsFile = XdgPaths.cacheDir.resolve("clubs.txt"),
      recentJobsFile = XdgPaths.cacheDir.resolve("recent-jobs.txt"),
      clientLayer = liveClient
    )

  // `HttpClientLayer.live`, not `Client.default`, for its 10s connect timeout (#182): a black-holed server fails in
  // Netty, below the uninterruptible unwind a ZIO `.timeout` would hit, leaving slow calls and streaming follows alone.
  // Suspended so that, as before #296, only a command that asks a server initialises `HttpClientLayer`.
  private val liveClient: ZLayer[Any, Throwable, Client] = ZLayer.suspend(HttpClientLayer.live)
}
