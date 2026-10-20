package ccas.server.routes

import com.augustnagro.magnum.sql
import zio.http.*
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec}
import zio.ZIO

import ccas.server.routes.RouteHelpers.{jsonResponse, renderError}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

object HealthRoutes {

  /** The `/health/ready` body, on a 200 and a 503 alike: the database the server's pool connects to, so a client can
    * say which one the server is on (#320). Absent when the configured URL has no address the server can read.
    */
  private[ccas] case class ReadyResponse(@jsonField("database") databaseOption: Option[String])
  object ReadyResponse {
    given JsonCodec[ReadyResponse] = DeriveJsonCodec.gen
  }

  val routes: Routes[PostgresClient, Nothing] = Routes(
    Method.GET / "health" -> handler(Response.ok),
    Method.GET / "health" / "ready" -> handler {
      for {
        addressOption <- ZIO.serviceWith[PostgresClient](_.addressOption)
        status <- connectZIO(sql"SELECT 1".query[Int].run())
                    .as(Status.Ok)
                    .orElseSucceed(Status.ServiceUnavailable)
      } yield jsonResponse(status, ReadyResponse(databaseOption = addressOption.map(_.render)))
    }
  ).handleErrorRequestCauseZIO(renderError)
}
