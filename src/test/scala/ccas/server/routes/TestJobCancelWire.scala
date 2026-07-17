package ccas.server.routes

import zio.*
import zio.http.{Client, Server}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.JobRunId
import ccas.cli.{CcasApiClient, CliError}
import ccas.server.jobs.{JobKind, JobRun, JobRunner, JobRunStatus}
import ccas.server.routes.JobRoutes.CancelResult
import ccas.server.ServerTables
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.ProgressDisplay

/** End-to-end wire smoke for job cancellation: a REAL zio-http server bound to an ephemeral port serving the REAL
  * `JobRoutes`, driven by the REAL `CcasApiClient` (the CLI's HTTP client) over the loopback, backed by the REAL
  * `JobRunner.live`. Exercises the whole `ccas cancel` path — client JSON encode/route → `runner.cancel` → fiber
  * interrupt → `Cancelled` terminal write → `CancelResult` decode — and the 404 branch. The one non-production seam is
  * the job body (`ZIO.never`, to hold a job Running) and that it is submitted via the runner rather than an HTTP submit
  * route; both submit routes and the analysis apps are covered by their own suites. `Server.install` binds the shared
  * server once (the real port even from configured port 0) and keeps serving under the `Server` layer's lifetime.
  */
object TestJobCancelWire extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobCancelWire")(
    testCancelLiveJobOverHttp,
    testCancelUnknownJobOverHttp
  ).provideShared(
    FreshSchemaLayer("test_cancel_wire", onInit = ServerTables.ensureTables),
    TestChessComClientSupport.dummyLayer,
    JobRunner.live,
    ZLayer.succeed(ProgressDisplay.make(enabled = false)),
    Server.defaultWith(_.port(0)),
    boundPort,
    Client.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)

  // The shared server's actual bound port, wrapped so it can't collide with any stray `Int` in the environment.
  private final case class ServerPort(value: Int)

  // Install the real JobRoutes on the shared server ONCE and expose its actual bound port (real port even from the
  // configured port 0). `install` needs no caller Scope — the server keeps serving under the `Server` layer's lifetime —
  // so `fromZIO` fits; installing per-test would re-register the routes and log a benign "Duplicate routes" warning.
  private val boundPort = ZLayer.fromZIO(Server.install(JobRoutes.routes).map(ServerPort(_)))

  // 127.0.0.1 (not "localhost") to avoid an IPv6/IPv4 loopback mismatch against the IPv4-bound test server.
  private def apiFor(port: ServerPort): URIO[Client, CcasApiClient] =
    CcasApiClient.live(s"http://127.0.0.1:${port.value}")

  private def awaitCancelled(runner: JobRunner, id: JobRunId): ZIO[PostgresClient, Throwable, JobRun] =
    runner.status(id).flatMap {
      case Some(job) if job.status != JobRunStatus.Running => ZIO.succeed(job)
      case _                                               => ZIO.sleep(50.millis) *> awaitCancelled(runner, id)
    }.timeoutFail(new Exception(s"job $id did not leave Running"))(10.seconds)

  private def testCancelLiveJobOverHttp =
    test("POST /api/jobs/{id}/cancel over real HTTP cancels a live job and returns CancelResult") {
      for {
        runner  <- ZIO.service[JobRunner]
        port    <- ZIO.service[ServerPort]
        api     <- apiFor(port)
        started <- Promise.make[Nothing, Unit]
        // A real live job: announces it is running, then blocks forever until the interrupt lands.
        id     <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => started.succeed(()) *> ZIO.never)
        _      <- started.await
        result <- api.postEmpty[CancelResult](s"/api/jobs/${JobRunId.unwrap(id)}/cancel")
        job    <- awaitCancelled(runner, id)
      } yield assertTrue(
        result.jobId == JobRunId.unwrap(id),
        job.status == JobRunStatus.Cancelled,
        job.completedAt.isDefined,
        job.error.exists(_.contains("Cancelled"))
      )
    }

  private def testCancelUnknownJobOverHttp =
    test("POST /api/jobs/{id}/cancel for an unknown job surfaces the 404 as a CliError") {
      for {
        port   <- ZIO.service[ServerPort]
        api    <- apiFor(port)
        result <- api.postEmpty[CancelResult]("/api/jobs/does-not-exist/cancel").either
      } yield assertTrue(result match {
        case Left(e: CliError) => e.message.contains("No running job") && e.exitCode == 1
        case _                 => false
      })
    }
}
