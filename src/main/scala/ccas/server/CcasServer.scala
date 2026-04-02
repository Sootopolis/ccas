package ccas.server

import com.typesafe.config.ConfigFactory
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.{Client, Server}

import ccas.server.jobs.JobRunner
import ccas.server.routes.{BlacklistRoutes, HealthRoutes, JobRoutes, ScheduleRoutes}
import ccas.server.scheduler.JobScheduler
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient
import ccas.utils.CcasLogger

object CcasServer extends ZIOAppDefault {

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] = {
    val config = ConfigFactory.load()
    val port   = config.getInt("server.port")

    (for {
      scheduler <- ZIO.service[JobScheduler]
      _         <- scheduler.start
      _ <- Server.serve(HealthRoutes.routes ++ JobRoutes.routes ++ ScheduleRoutes.routes ++ BlacklistRoutes.routes)
    } yield ()).provideSome[Scope](
      CcasLogger.live(showProgress = false),
      ChessComClient.live("server"),
      Client.default,
      PostgresClient.live(onInit = ServerTables.ensureTables),
      JobRunner.live,
      JobScheduler.live,
      Server.defaultWithPort(port)
    )
  }
}
