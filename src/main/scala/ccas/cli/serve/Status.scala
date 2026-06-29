package ccas.cli.serve

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.Path
import java.time.{Duration => JDuration}

import com.typesafe.config.ConfigFactory
import zio.{Console, ExitCode, UIO, ZIO}

/** `ccas server status`: report whether the local ccas server is up. Combines two signals — an HTTP probe of the
  * loopback health endpoints (universal: catches a foreground server that writes no pid file) and the pid file (adds
  * the detached server's pid). The decision logic lives in the pure [[describe]] so it is unit-testable without a real
  * server or process (same pattern as [[PidFile.alreadyRunning]]); [[run]] only wires the effects.
  */
object Status {

  private val DefaultPort    = 8080
  private val ConnectTimeout = JDuration.ofSeconds(2)
  private val LiveTimeout    = JDuration.ofSeconds(2)
  // `/health/ready` runs a `SELECT 1`; on a scale-to-zero database (Neon) the first query wakes the compute and can
  // take several seconds, so give the readiness probe a wider window than the plain liveness probe to avoid a false
  // "db unavailable" on a healthy-but-cold server.
  private val ReadyTimeout = JDuration.ofSeconds(10)

  /** Map the probe + pid signals to a one-line message and exit code. Exit 0 only when the server is ready (db ok);
    * every other state (degraded, starting, down) is exit 1, so `ccas server status && …` gates on a healthy server.
    *
    * @param ready    `/health/ready` returned 200 (server up AND database reachable)
    * @param up       `/health` returned 200 (server process responding; db state unknown)
    * @param livePid  the pid from the pid file, only when it maps to a live process
    * @param stalePid the pid file exists but its pid is dead
    */
  def describe(ready: Boolean, up: Boolean, livePid: Option[Long], stalePid: Boolean, port: Int): (String, ExitCode) = {
    val addr   = s"127.0.0.1:$port"
    val pidStr = livePid.map(p => s"pid $p  ").getOrElse("")
    if (ready) {
      (s"running (ready)  ${pidStr}${addr}  db ok", ExitCode.success)
    } else if (up) {
      (s"running (db unavailable)  ${pidStr}${addr}", ExitCode(1))
    } else {
      livePid match {
        case Some(p) => (s"starting or unhealthy (pid $p, no response on :$port)", ExitCode(1))
        case None    => (if (stalePid) { "not running (stale pid file)" } else { "not running" }, ExitCode(1))
      }
    }
  }

  /** GET the given path on the loopback port; true only on a 200. Any transport error (connection refused, timeout)
    * counts as "not 200" — a down or unreachable server. Probes 127.0.0.1: the server's default bind is loopback, and
    * even a `SERVER_HOST=0.0.0.0` bind answers on loopback; a non-loopback-only bind addr would false-negative here,
    * which is acceptable under the single-user local model. */
  private def probe(client: HttpClient, port: Int, path: String, timeout: JDuration): UIO[Boolean] =
    ZIO
      .attemptBlocking {
        val req = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path")).timeout(timeout).GET().build()
        client.send(req, HttpResponse.BodyHandlers.discarding()).statusCode() == 200
      }
      .orElseSucceed(false)

  def run(pidPath: Path): UIO[ExitCode] = {
    val client = HttpClient.newBuilder().connectTimeout(ConnectTimeout).build()
    for {
      port         <- ZIO.attemptBlocking(ConfigFactory.load().getInt("server.port")).orElseSucceed(DefaultPort)
      ready        <- probe(client, port, "/health/ready", ReadyTimeout)
      up           <- if (ready) { ZIO.succeed(true) } else { probe(client, port, "/health", LiveTimeout) }
      pidRead      <- ZIO.attemptBlocking(PidFile.read(pidPath)).orElseSucceed(Option.empty[Long])
      alive         = pidRead.exists(PidFile.isAlive)
      (msg, code)   = describe(ready, up, pidRead.filter(_ => alive), pidRead.isDefined && !alive, port)
      _            <- Console.printLine(msg).orDie
    } yield code
  }
}
