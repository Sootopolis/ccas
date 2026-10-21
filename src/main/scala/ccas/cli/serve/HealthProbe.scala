package ccas.cli.serve

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.{Duration => JDuration}

import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory}
import zio.{IO, UIO, ZIO}
import zio.json.DecoderOps

import ccas.server.routes.HealthRoutes.ReadyResponse

/** Shared loopback health-probe used by [[ServerStatus]] (to report running/ready), [[Detach]] (to refuse a port
  * already answering, then wait for the child to be ready) and [[ServerStop]] (to warn that a foreground server is
  * still up after `down` finds no detached pid).
  * Centralising the port resolution and the `/health` liveness probe makes the "`down`'s hint fires exactly when
  * `status` says running" invariant structural rather than a copy-paste promise — change the config key or a timeout
  * here and every caller moves with it.
  */
object HealthProbe {

  /** What `/health/ready` answered. `databaseOption` is the database the server named, absent from a server too old
    * to name one or one that can't read its own address.
    */
  enum Readiness {
    case Ready(databaseOption: Option[String])
    case Unready(databaseOption: Option[String])
    case NoAnswer
  }

  private val ConnectTimeout = JDuration.ofSeconds(2)
  private val LiveTimeout    = JDuration.ofSeconds(2)
  // `/health/ready` runs a `SELECT 1`; on a scale-to-zero database (Neon) the first query wakes the compute and can
  // take several seconds, so give readiness a wider window than plain liveness to avoid a false "db unavailable".
  private val ReadyTimeout = JDuration.ofSeconds(10)

  // One reusable client (thread-safe, designed for reuse) rather than one per probe; `status` makes two probes.
  private val client = HttpClient.newBuilder().connectTimeout(ConnectTimeout).build()

  /** The server's bound port from `server.port`. Relies on the caller having applied the ccas.env overlay first so a
    * file-only `SERVER_PORT` is honoured. A value that isn't a port fails rather than falling back to the default,
    * since the port picks which server's pid file `down` signals (#326).
    */
  def resolvePort: IO[String, Int] =
    ZIO.attemptBlocking(ConfigFactory.load()).mapError(messageOf).flatMap(config => ZIO.fromEither(portIn(config)))

  /** `server.port` from `config`, or why it isn't a port. Pure. */
  private[serve] def portIn(config: Config): Either[String, Int] =
    Try(config.getInt("server.port")).toEither.left
      .map(e => s"cannot read server.port (SERVER_PORT): ${messageOf(e)}")
      .filterOrElse(port => port >= 1 && port <= 65535, "server.port (SERVER_PORT) must be a port from 1 to 65535")

  private def messageOf(t: Throwable): String = Option(t.getMessage).getOrElse(t.getClass.getSimpleName)

  /** A GET of `path` on the loopback `port`. Probes 127.0.0.1: the server's default bind is loopback, and even a
    * `SERVER_HOST=0.0.0.0` bind answers on loopback; a non-loopback-only bind would false-negative here, acceptable
    * under the single-user local model. */
  private def request(port: Int, path: String, timeout: JDuration): HttpRequest =
    HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path")).timeout(timeout).GET().build()

  /** `/health/ready` with the wide timeout a cold database needs. */
  def readiness(port: Int): UIO[Readiness] = readinessWithin(port, ReadyTimeout)

  /** `/health/ready` with a caller-chosen timeout, for a caller that polls. A transport error (connection refused,
    * timeout) is [[Readiness.NoAnswer]] — a down or unreachable server. */
  def readinessWithin(port: Int, timeout: JDuration): UIO[Readiness] =
    ZIO
      .attemptBlocking {
        val response = client.send(request(port, "/health/ready", timeout), HttpResponse.BodyHandlers.ofString())
        classify(response.statusCode(), response.body())
      }
      .orElseSucceed(Readiness.NoAnswer)

  /** Read a `/health/ready` response. A body that does not decode names no database: an older server sends none.
    * Pure. */
  def classify(status: Int, body: String): Readiness = {
    val databaseOption = body.fromJson[ReadyResponse].toOption.flatMap(_.databaseOption)
    status match {
      case 200 => Readiness.Ready(databaseOption)
      case 503 => Readiness.Unready(databaseOption)
      case _   => Readiness.NoAnswer
    }
  }

  /** `/health` returned 200 — server process responding, database state unknown. Any transport error is "not 200". */
  def isUp(port: Int): UIO[Boolean] =
    ZIO
      .attemptBlocking {
        client.send(request(port, "/health", LiveTimeout), HttpResponse.BodyHandlers.discarding()).statusCode() == 200
      }
      .orElseSucceed(false)
}
