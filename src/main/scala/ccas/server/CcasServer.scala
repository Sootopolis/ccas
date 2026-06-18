package ccas.server

import java.nio.file.{Files, Paths}

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
    val host   = config.getString("server.host")
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
      _         <- pidFileManaged
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
      Server.defaultWith(_.binding(host, port))
    )
  }

  /** When launched detached (`ccas serve --detach`), the CLI parent passes the pid-file path via `CCAS_PID_FILE`. We
    * write our OWN pid here on boot and delete it on shutdown — the server owns its pid-file lifecycle, so the path is
    * computed CLI-side (no `ccas.cli` import / package cycle) and the recorded pid is the real server JVM, not an
    * intermediate `setsid` process. Foreground `ccas serve` and the `ccas-server` deployable don't set the env, so
    * this is a no-op for them. Sequenced before `Server.serve` so the file exists by the time `/health/ready` is 200;
    * the release runs on SIGTERM because ZIOAppDefault's shutdown hook interrupts this scope.
    */
  private val pidFileManaged: ZIO[Scope, Throwable, Unit] =
    sys.env.get("CCAS_PID_FILE").map(_.trim).filter(_.nonEmpty) match {
      case None => ZIO.unit
      case Some(p) =>
        val path = Paths.get(p)
        ZIO
          .acquireRelease(
            ZIO.attemptBlocking {
              Option(path.getParent).foreach(Files.createDirectories(_))
              Files.writeString(path, ProcessHandle.current().pid().toString + "\n")
            }
          )(_ => ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
          .unit
    }
}
