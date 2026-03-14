package ccas.server.routes

import com.augustnagro.magnum.{sql, Transactor}
import zio.ZIO
import zio.http.*

import ccas.utils.sql.SqlZioTypes.connectZIO

object HealthRoutes {

  val routes: Routes[Transactor, Nothing] = Routes(
    Method.GET / "health" -> handler(Response.ok),

    Method.GET / "health" / "ready" -> handler {
      connectZIO { sql"SELECT 1".query[Int].run() }
        .as(Response.ok)
        .orElse(ZIO.succeed(Response(status = Status.ServiceUnavailable)))
    }
  )
}
