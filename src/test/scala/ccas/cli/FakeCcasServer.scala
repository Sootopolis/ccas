package ccas.cli

import zio.{Scope, Trace, UIO, ULayer, ZIO, ZLayer}
import zio.http.*
import zio.json.{EncoderOps, JsonEncoder}

import ccas.analysis.apps.{ClubQuery, ClubResolution}
import ccas.server.routes.{ClubRequest, ClubResult}
import ccas.server.routes.ClubRoutes.ClubInfo

/** A `ccas` server that answers from in-memory routes instead of a socket, so a CLI command can be driven against
  * canned answers — encoded with the server's own wire types — with no database, no port and no real server.
  */
object FakeCcasServer {

  /** The base URL to hand a command. The host is never resolved: only the path is routed. */
  val Url = "http://ccas.test"

  def layer(routes: Routes[Any, Response]): ULayer[Client] = ZLayer.succeed(client(routes))

  def api(routes: Routes[Any, Response]): UIO[CcasApiClient] = CcasApiClient.live(Url).provide(layer(routes))

  def json[A: JsonEncoder](body: A): Response = Response.json(body.toJson)

  /** The resolve route, answering `resolution` — with `name` as the club's full name — for exactly `query`, and 400 for
    * anything else, so a test pins what the command asked as well as what it did with the answer.
    */
  def resolving(query: ClubQuery, resolution: ClubResolution, name: String): Route[Any, Nothing] =
    Method.GET / "api" / "clubs" / "resolve" -> handler { (req: Request) =>
      val info = resolution.runnable.toOption.map(club => ClubInfo(club.slug, name))
      ClubRequest
        .query(req)
        .fold(
          _ => Response.status(Status.BadRequest),
          asked =>
            if (asked == query) { json(ClubResult(query.describe, resolution, info)) }
            else { Response.status(Status.BadRequest) }
        )
    }

  private def client(routes: Routes[Any, Response]): Client =
    ZClient.fromDriver(new ZClient.Driver[Any, Scope, Throwable] {
      override def request(
        version: Version,
        method: Method,
        url: URL,
        headers: Headers,
        body: Body,
        sslConfig: Option[ClientSSLConfig],
        proxy: Option[Proxy]
      )(implicit trace: Trace): ZIO[Scope, Throwable, Response] =
        routes.runZIO(Request(method = method, url = url, headers = headers, body = body))

      override def socket[Env1 <: Any](
        version: Version,
        url: URL,
        headers: Headers,
        app: WebSocketApp[Env1]
      )(implicit
        trace: Trace,
        ev: Scope =:= Scope
      ): ZIO[Env1 & Scope, Throwable, Response] =
        ZIO.die(new UnsupportedOperationException("the fake ccas server serves no websockets"))
    })
}
