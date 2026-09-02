package ccas.server

import java.nio.file.{Files, Paths}

import com.typesafe.config.ConfigFactory
import zio.{durationInt, Schedule, Scope, URIO, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.{Routes, Server}

import ccas.analysis.tables.Tables
import ccas.server.config.{ServerEnvOverlay, ServerEnvPaths}
import ccas.server.jobs.JobRunner
import ccas.server.routes.{
  BlacklistRoutes,
  ClubRoutes,
  HealthRoutes,
  JobRoutes,
  ManagedClubRoutes,
  RecruitmentCriteriaRoutes,
  ScheduleRoutes
}
import ccas.server.scheduler.JobScheduler
import ccas.utils.ProgressDisplay
import ccas.utils.client.{BodyStore, ChessComClient, HttpClientLayer}
import ccas.utils.sql.PostgresClient

object CcasServer extends ZIOAppDefault {

  // Apply the ccas.env overlay (file → system properties for any env var not already set) BEFORE the first
  // ConfigFactory.load below, so the standalone `ccas-server` binary (and the systemd unit) boot from `ccas config`'s
  // file without hand-exported env. `suspendSucceed` defers the config read until the overlay effect has run. Foreground
  // `ccas serve` already runs the overlay in `Main` before this; the only-if-absent guard makes this call idempotent.
  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    ServerEnvOverlay(ServerEnvPaths.file) *> ZIO.suspendSucceed {
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
          ManagedClubRoutes.routes,
          ClubRoutes.routes
        ).reduce(_ ++ _)

      (for {
        _         <- pidFileManaged
        scheduler <- ZIO.service[JobScheduler]
        _         <- scheduler.start
        _         <- retentionSweepForked
        _         <- Server.serve(routes)
      } yield ()).provideSome[Scope](
        ProgressDisplay.live(showProgress = false),
        ChessComClient.live("server"),
        HttpClientLayer.live,
        BodyStore.live,
        PostgresClient.live(onInit = ServerTables.ensureTables),
        JobRunner.live,
        JobScheduler.live,
        // Read-idle reaper: zio-http leaves `idleTimeout=None`, so a client that vanishes without FIN/RST would pin
        // a Netty channel and fd forever. Read-only, so it never resets on writes — which means every live log/progress
        // follow is reaped on this schedule and reconnects transparently (#161).
        // See docs/adr/0015-server-read-idle-reaper.md before changing the duration.
        Server.defaultWith(_.binding(host, port).idleTimeout(60.seconds))
      )
    }

  /** One pass of every retention sweep this server runs: the job-log files, then the two API-diagnostics tables.
    * Each half is caught on its own — one bad pass must not end the loop, and neither half's failure may skip the
    * other — and neither is fatal: a stale cache row costs one conditional GET, so nothing here is worth refusing to
    * serve over. The file sweep goes first because it is bounded by one directory walk, where the table half frees an
    * unbounded number of body objects at a round trip each.
    */
  private[server] val retentionPass: URIO[PostgresClient & BodyStore & JobRunner, Unit] =
    ZIO
      .serviceWithZIO[JobRunner](_.sweepLogs)
      .catchAllCause(cause => ZIO.logErrorCause("Job-log sweep failed", cause)) *>
      Tables.retentionSweep.catchAllCause(cause => ZIO.logErrorCause("Retention sweep failed", cause))

  /** Forked so the port binds without waiting for a pass, and daily so no pass has more than a day of backlog to
    * clear. See docs/adr/0007-response-caching-in-postgres.md. Shutdown interrupts whatever pass is in flight; for
    * the table half that leaks objects, which the bucket lifecycle rule in ADR 0009 exists to collect (an interrupted
    * file sweep leaks nothing — the files remain).
    */
  private val retentionSweepForked: URIO[Scope & PostgresClient & BodyStore & JobRunner, Unit] =
    retentionPass.repeat(Schedule.fixed(RetentionSweepInterval)).forkScoped.unit

  // Daily, because every retention window is day-granular. Each pass re-reads `app_setting`, so a retention change
  // takes effect within a day rather than at the next restart.
  private val RetentionSweepInterval = 24.hours

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
