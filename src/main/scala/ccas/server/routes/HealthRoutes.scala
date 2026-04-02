package ccas.server.routes

import com.augustnagro.magnum.sql
import zio.http.*
import zio.ZIO

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

object HealthRoutes {

  val routes: Routes[PostgresClient, Nothing] = Routes(
    Method.GET / "health" -> handler(Response.ok),
    Method.GET / "health" / "ready" -> handler {
      connectZIO(sql"SELECT 1".query[Int].run())
        .as(Response.ok)
        .orElse(ZIO.succeed(Response(status = Status.ServiceUnavailable)))
    }
  )
}
