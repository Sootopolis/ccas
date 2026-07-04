package ccas.cli.serve

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.{Duration => JDuration}

import com.typesafe.config.ConfigFactory
import zio.{UIO, ZIO}

/** Shared loopback health-probe used by both [[Status]] (to report running/ready) and [[Stop]] (to warn that a
  * foreground server is still up after `down` finds no detached pid). Centralising the port resolution and the
  * `/health` liveness probe makes the "`down`'s hint fires exactly when `status` says running" invariant structural
  * rather than a copy-paste promise — change the config key or a timeout here and both callers move together.
  */
object HealthProbe {

  private val DefaultPort    = 8080
  private val ConnectTimeout = JDuration.ofSeconds(2)
  private val LiveTimeout    = JDuration.ofSeconds(2)
  // `/health/ready` runs a `SELECT 1`; on a scale-to-zero database (Neon) the first query wakes the compute and can
  // take several seconds, so give readiness a wider window than plain liveness to avoid a false "db unavailable".
  private val ReadyTimeout = JDuration.ofSeconds(10)

  // One reusable client (thread-safe, designed for reuse) rather than one per probe; `status` makes two probes.
  private val client = HttpClient.newBuilder().connectTimeout(ConnectTimeout).build()

  /** The server's bound port from `server.port`, falling back to [[DefaultPort]] when the key is absent/unreadable.
    * Relies on the caller having applied the ccas.env overlay first so a file-only `SERVER_PORT` is honoured. */
  def resolvePort: UIO[Int] =
    ZIO.attemptBlocking(ConfigFactory.load().getInt("server.port")).orElseSucceed(DefaultPort)

  /** GET `path` on the loopback `port`; true only on a 200. Any transport error (connection refused, timeout) counts
    * as "not 200" — a down or unreachable server. Probes 127.0.0.1: the server's default bind is loopback, and even a
    * `SERVER_HOST=0.0.0.0` bind answers on loopback; a non-loopback-only bind would false-negative here, acceptable
    * under the single-user local model. */
  private def probe(port: Int, path: String, timeout: JDuration): UIO[Boolean] =
    ZIO
      .attemptBlocking {
        val req = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path")).timeout(timeout).GET().build()
        client.send(req, HttpResponse.BodyHandlers.discarding()).statusCode() == 200
      }
      .orElseSucceed(false)

  /** `/health/ready` returned 200 — server up AND database reachable. Wider timeout for cold-DB wake-up. */
  def isReady(port: Int): UIO[Boolean] = probe(port, "/health/ready", ReadyTimeout)

  /** `/health` returned 200 — server process responding, database state unknown. */
  def isUp(port: Int): UIO[Boolean] = probe(port, "/health", LiveTimeout)
}
