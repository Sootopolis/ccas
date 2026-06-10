package ccas.server

import com.typesafe.config.ConfigFactory
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.{Routes, Server}

import ccas.server.jobs.JobRunner
import ccas.server.routes.{BlacklistRoutes, ClubRoutes, HealthRoutes, JobRoutes, RecruitmentCriteriaRoutes, ScheduleRoutes}
import ccas.server.scheduler.JobScheduler
import ccas.utils.ProgressDisplay
import ccas.utils.client.{ChessComClient, HttpClientLayer}
import ccas.utils.sql.PostgresClient

object CcasServer extends ZIOAppDefault {

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] = {
    val config = ConfigFactory.load()
    val port   = config.getInt("server.port")

    val routes: Routes[JobRunner & ChessComClient & PostgresClient, Nothing] =
      List(
        HealthRoutes.routes,
        JobRoutes.routes,
        ScheduleRoutes.routes,
        BlacklistRoutes.routes,
        RecruitmentCriteriaRoutes.routes,
        ClubRoutes.routes
      ).reduce(_ ++ _)

    (for {
      scheduler <- ZIO.service[JobScheduler]
      _         <- scheduler.start
      _         <- Server.serve(routes)
    } yield ()).provideSome[Scope](
      ProgressDisplay.live(showProgress = false),
      ChessComClient.live("server"),
      HttpClientLayer.live,
      PostgresClient.live(onInit = ServerTables.ensureTables),
      JobRunner.live,
      JobScheduler.live,
      Server.defaultWithPort(port)
    )
  }
}
