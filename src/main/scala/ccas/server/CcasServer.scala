package ccas.server

import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.ConfigFactory
import zio.{durationInt, ExitCode, RIO, Schedule, Scope, URIO, ZIO, ZIOAppArgs, ZIOAppDefault, ZLayer}
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

  // The standalone `ccas-server`. A declined boot exits 1 here rather than failing, which ZIOApp would log as an error
  // with a stack trace. `exit` is safe only in the app that owns the JVM: called under `ccas.cli.Main`, it would
  // deadlock with Main's shutdown hook, so `ccas server up` runs [[serve]] and handles `Declined` itself.
  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    serve.catchSome { case BootGuard.Declined => exit(ExitCode(1)) }

  // Apply the ccas.env overlay (file → system properties for any env var not already set) BEFORE the first
  // ConfigFactory.load below, so the standalone `ccas-server` binary (and the systemd unit) boot from `ccas config`'s
  // file without hand-exported env. `suspendSucceed` defers the config read until the overlay effect has run.
  // Foreground `ccas server up` already runs the overlay in `Main` before this; the only-if-absent guard makes this call
  // idempotent.
  private[ccas] def serve: ZIO[Scope, Throwable, Unit] =
    ServerEnvOverlay(ServerEnvPaths.file) *>
      ZIO.ifZIO(BootGuard.confirmed(detachedChild = detachedPidFileOption.isDefined))(
        onTrue = boot,
        onFalse = ZIO.fail(BootGuard.Declined)
      )

  // Behind the guard in `run`, since building these layers writes to the database: the pool's `onInit`, then
  // `JobRunner.live`.
  private def boot: ZIO[Scope, Throwable, Unit] =
    ZIO.suspendSucceed {
      val config = ConfigFactory.load()
      val host   = config.getString("server.host")
      val port   = config.getInt("server.port")

      serveWith(
        pidFileOption = detachedPidFileOption,
        services = ZLayer.makeSome[ProgressDisplay, Served](
          ChessComClient.live("server"),
          HttpClientLayer.live,
          BodyStore.live,
          PostgresClient.live(onInit = poolInit),
          JobRunner.live,
          JobScheduler.live
        ),
        // Read-idle reaper: zio-http leaves `idleTimeout=None`, so a client that vanishes without FIN/RST would pin
        // a Netty channel and fd forever. Read-only, so it never resets on writes — which means every live log/progress
        // follow is reaped on this schedule and reconnects transparently (#161).
        // See docs/adr/0015-server-read-idle-reaper.md before changing the duration.
        server = Server.defaultWith(_.binding(host, port).idleTimeout(60.seconds))
      )
    }

  /** The services [[boot]] builds for the HTTP server and the loops to run on. */
  private[server] type Served = JobScheduler & JobRunner & PostgresClient & BodyStore & ChessComClient

  // The pid file stays in the caller's scope, so it outlives the services. The rest gets a scope of its own, closed
  // before the services' one (#334), and acquired so that shutdown stops the loops, then the jobs, which ends their
  // followers' requests, then the HTTP server, whose drain would otherwise wait on those requests.
  private[server] def serveWith(
    pidFileOption: Option[Path],
    services: ZLayer[ProgressDisplay, Throwable, Served],
    server: ZLayer[Any, Throwable, Server]
  ): ZIO[Scope, Throwable, Unit] =
    (pidFileManaged(pidFileOption) *> ZIO.scoped {
      for {
        serverEnv <- server.build
        _         <- ZIO.addFinalizer(ZIO.serviceWithZIO[JobRunner](_.interruptAll))
        _         <- running.provideSomeEnvironment[Scope & Served](_ ++ serverEnv)
      } yield ()
    }).provideSomeLayer[Scope](
      // Before the services, not beside them: a layer built beside the display logs through ZIO's default logger, as
      // the pool's `Database:` line did (#337).
      ProgressDisplay.live(showProgress = false) >>> services
    )

  private def running: URIO[Scope & Served & Server, Unit] = {
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

    for {
      scheduler <- ZIO.service[JobScheduler]
      _         <- scheduler.start
      _         <- retentionSweepForked
      _         <- Server.serve(routes)
    } yield ()
  }

  /** The pool's `onInit`: names the database, then applies the tables. The pool is lazy by default
    * (`initializationFailTimeout = -1`), so naming it first means even a boot that then fails to connect says where it
    * tried (#320).
    */
  private[server] val poolInit: RIO[PostgresClient, Unit] =
    ZIO.serviceWithZIO[PostgresClient] { client =>
      ZIO.logInfo(s"Database: ${client.addressOption.fold("address not readable from the configuration")(_.render)}")
    } *> ServerTables.ensureTables

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

  // Daily, because every retention window is day-granular. Each pass re-reads `app_setting`, so a retention change
  // takes effect within a day rather than at the next restart.
  private val RetentionSweepInterval = 24.hours

  /** Forked so the port binds without waiting for a pass, and daily so no pass has more than a day of backlog to
    * clear. See docs/adr/0007-response-caching-in-postgres.md. Shutdown interrupts whatever pass is in flight; for
    * the table half that leaks objects, which the bucket lifecycle rule in ADR 0009 exists to collect (an interrupted
    * file sweep leaks nothing — the files remain).
    */
  private val retentionSweepForked: URIO[Scope & PostgresClient & BodyStore & JobRunner, Unit] =
    retentionPass.repeat(Schedule.fixed(RetentionSweepInterval)).forkScoped.unit

  /** Set only on a `--detach` child, by its parent: the pid-file path, as [[pidFileManaged]] says. */
  private[ccas] val PidFileEnv = "CCAS_PID_FILE"

  private val detachedPidFileOption: Option[Path] =
    sys.env.get(PidFileEnv).map(_.trim).filter(_.nonEmpty).map(Paths.get(_))

  /** Writes this JVM's pid to the pid file on boot and deletes it on shutdown, for a server started by `ccas server up
    * --detach`, whose parent passes the path in [[PidFileEnv]]. The server owns its pid-file lifecycle, so the path is
    * computed CLI-side (no `ccas.cli` import / package cycle) and the recorded pid is the real server JVM, not an
    * intermediate `setsid` process. Foreground `ccas server up` and the `ccas-server` deployable pass no path, so this
    * is a no-op for them. Sequenced before `Server.serve` so the file exists by the time `/health/ready` is 200; the
    * release runs on SIGTERM because ZIOAppDefault's shutdown hook interrupts this scope.
    */
  private[server] def pidFileManaged(pidFileOption: Option[Path]): ZIO[Scope, Throwable, Unit] =
    ZIO.foreachDiscard(pidFileOption) { path =>
      ZIO.acquireRelease(
        ZIO.attemptBlocking {
          Option(path.getParent).foreach(Files.createDirectories(_))
          Files.writeString(path, ProcessHandle.current().pid().toString + "\n")
        }
      )(_ => ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
    }
}
