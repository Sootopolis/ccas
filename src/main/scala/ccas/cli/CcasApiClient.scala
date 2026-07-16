package ccas.cli

import zio.*
import zio.http.*
import zio.json.*
import zio.stream.{ZPipeline, ZStream}

import ccas.server.jobs.JobLogStream
import ccas.utils.ProgressSnapshot
import ccas.utils.errors.ErrorResponse

/** Thin HTTP client to a local `CcasServer`. Every method fails with [[CliError]] (carrying an exit code) so the
  * dispatcher can render a single message and exit cleanly. JSON bodies use the server's own wire DTOs.
  */
trait CcasApiClient {
  def getJson[Resp: JsonDecoder](path: String): Task[Resp]
  def postJson[Req: JsonEncoder, Resp: JsonDecoder](path: String, body: Req): Task[Resp]
  def postEmpty[Resp: JsonDecoder](path: String): Task[Resp]
  def postUnit[Req: JsonEncoder](path: String, body: Req): Task[Unit]
  def delete(path: String): Task[Unit]

  /** Stream a chunked `text/plain` endpoint line by line, invoking `onLine` for each line as it arrives. Used to
    * follow a job's live log output; scoped internally so callers need no `Scope`.
    */
  def streamLines(path: String)(onLine: String => UIO[Unit]): Task[Unit]

  /** Stream a job's `/progress` NDJSON endpoint, invoking `onFrame` for each decoded [[ProgressSnapshot]]. A line that
    * fails to decode is skipped (never fails the stream) — bar rendering is best-effort. Transport errors propagate as
    * for [[streamLines]]; the follow path treats them as non-fatal (bars just stop).
    */
  def streamProgress(path: String)(onFrame: ProgressSnapshot => UIO[Unit]): Task[Unit]
}

object CcasApiClient {
  def live(baseUrl: String): URIO[Client, CcasApiClient] =
    ZIO.serviceWith[Client](client => new CcasApiClientLive(client, baseUrl))

  private final class CcasApiClientLive(client: Client, baseUrl: String) extends CcasApiClient {

    // `batched` reads the full response body and manages its own connection scope, so callers need no Scope.
    private val http = client.batched

    private def rootMessage(e: Throwable): String =
      Option(e.getMessage).getOrElse(e.getClass.getSimpleName)

    private def url(path: String): Task[URL] =
      ZIO.fromEither(URL.decode(baseUrl + path))
        .mapError(e => CliError(s"invalid server URL '$baseUrl$path': ${rootMessage(e)}", 2))

    /** Any transport-level failure means the server isn't reachable — non-2xx responses come back as a `Response`. */
    private def send(req: Request): Task[Response] =
      http(req).mapError(e =>
        CliError(s"cannot reach $baseUrl — start a server with 'ccas server up'. (${rootMessage(e)})", 1)
      )

    private def errorMessage(body: String, status: Status): String =
      body.fromJson[ErrorResponse].map(_.error).getOrElse {
        // Plain-text error bodies (e.g. the /logs 404) aren't JSON — surface the message itself, capped so a stray
        // HTML error page can't flood the terminal.
        val trimmed = body.trim
        if (trimmed.nonEmpty && trimmed.length <= 200) { trimmed } else { s"HTTP ${status.code}" }
      }

    private def decode[Resp: JsonDecoder](resp: Response): Task[Resp] =
      resp.body.asString.flatMap { s =>
        if (resp.status.isSuccess) {
          ZIO.fromEither(s.fromJson[Resp]).mapError(m => CliError(s"unexpected response from server: $m", 1))
        } else {
          ZIO.fail(CliError(errorMessage(s, resp.status), 1))
        }
      }

    private def ensureSuccess(resp: Response): Task[Unit] =
      ZIO.unlessDiscard(resp.status.isSuccess) {
        resp.body.asString.flatMap(s => ZIO.fail(CliError(errorMessage(s, resp.status), 1)))
      }

    private def jsonRequest(method: Method, u: URL, body: Option[String]): Request = {
      val base = Request(method = method, url = u, body = body.fold(Body.empty)(s => Body.fromString(s)))
      body.fold(base)(_ => base.addHeader(Header.ContentType(MediaType.application.json)))
    }

    override def getJson[Resp: JsonDecoder](path: String): Task[Resp] =
      url(path).flatMap(u => send(jsonRequest(Method.GET, u, None))).flatMap(decode[Resp])

    override def postJson[Req: JsonEncoder, Resp: JsonDecoder](path: String, body: Req): Task[Resp] =
      url(path).flatMap(u => send(jsonRequest(Method.POST, u, Some(body.toJson)))).flatMap(decode[Resp])

    override def postEmpty[Resp: JsonDecoder](path: String): Task[Resp] =
      url(path).flatMap(u => send(jsonRequest(Method.POST, u, None))).flatMap(decode[Resp])

    override def postUnit[Req: JsonEncoder](path: String, body: Req): Task[Unit] =
      url(path).flatMap(u => send(jsonRequest(Method.POST, u, Some(body.toJson)))).flatMap(ensureSuccess)

    override def delete(path: String): Task[Unit] =
      url(path).flatMap(u => send(jsonRequest(Method.DELETE, u, None))).flatMap(ensureSuccess)

    // `client.stream` keeps the connection open and surfaces the chunked body as a live byte stream (unlike `batched`,
    // which buffers the whole response — wrong for following a still-running job). A non-success status fails the
    // stream with the server's error message. `KeepAliveLine` frames (interleaved by the server so a silent job can't
    // idle the connection shut, #150) are filtered out here so callers only see real log lines. A transport failure is
    // split on the `connected` flag: if a 200 already reached us the stream dropped mid-follow (the job keeps running
    // server-side) → `StreamDropped`; otherwise we never reached the server → an unreachable `CliError`.
    override def streamLines(path: String)(onLine: String => UIO[Unit]): Task[Unit] =
      for {
        u         <- url(path)
        connected <- Ref.make(false)
        _ <- client
          .stream(Request(method = Method.GET, url = u)) { resp =>
            if (resp.status.isSuccess) {
              ZStream.fromZIO(connected.set(true)).drain ++
                resp.body.asStream
                  .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                  .filter(_ != JobLogStream.KeepAliveLine)
            } else {
              ZStream.fromZIO(resp.body.asString.flatMap(s => ZIO.fail(CliError(errorMessage(s, resp.status), 1))))
            }
          }
          .runForeach(onLine)
          .catchAll {
            case e: CliError => ZIO.fail(e)
            case e =>
              connected.get.flatMap { wasConnected =>
                if (wasConnected) { ZIO.fail(StreamDropped(rootMessage(e))) }
                else {
                  ZIO.fail(CliError(s"cannot reach $baseUrl — start a server with 'ccas server up'. (${rootMessage(e)})", 1))
                }
              }
          }
      } yield ()

    // Each `/progress` line is one `ProgressSnapshot` JSON object; a decode failure (should never happen) is dropped so
    // a malformed frame can't kill live bar rendering. `streamLines` already filters the keepalive frames.
    override def streamProgress(path: String)(onFrame: ProgressSnapshot => UIO[Unit]): Task[Unit] =
      streamLines(path)(line => ZIO.fromEither(line.fromJson[ProgressSnapshot]).foldZIO(_ => ZIO.unit, onFrame))
  }
}
