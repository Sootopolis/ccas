package ccas.cli

import zio.*
import zio.http.*
import zio.json.*
import zio.stream.{ZPipeline, ZStream}

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
        CliError(s"cannot reach $baseUrl. start a server with 'ccas serve' first. (${rootMessage(e)})", 1)
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
    // stream with the server's error message. Any other transport error covers both "never connected" and "connection
    // dropped mid-stream", so the message stays honest for both rather than asserting the server was unreachable.
    override def streamLines(path: String)(onLine: String => UIO[Unit]): Task[Unit] =
      url(path).flatMap { u =>
        client
          .stream(Request(method = Method.GET, url = u)) { resp =>
            if (resp.status.isSuccess) { resp.body.asStream.via(ZPipeline.utf8Decode >>> ZPipeline.splitLines) }
            else { ZStream.fromZIO(resp.body.asString.flatMap(s => ZIO.fail(CliError(errorMessage(s, resp.status), 1)))) }
          }
          .runForeach(onLine)
          .mapError {
            case e: CliError => e
            case e => CliError(s"lost connection to $baseUrl — is a server running? start one with 'ccas serve'. (${rootMessage(e)})", 1)
          }
      }
  }
}
