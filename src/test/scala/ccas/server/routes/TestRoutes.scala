package ccas.server.routes

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.{sql, Transactor}
import zio.{durationInt, Chunk, Duration, Fiber, RIO, Ref, Scope, Semaphore, Trace, UIO, URIO, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.ClubSlug
import ccas.server.jobs.*
import ccas.server.ServerTables
import ccas.utils.{CcasLogger, TestCcasLogger}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.SqlZioTypes.connectZIO

object TestRoutes extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRoutes")(
    suiteHealth,
    suiteJobRoutes,
    suiteScheduleRoutes
  ).provideShared(
    FreshSchemaLayer("test_routes", onInit = ServerTables.ensureTables),
    fakeJobRunnerLayer,
    dummyChessComClientLayer,
    Scope.default
  ) @@ TestAspect.sequential

  // --- Fake JobRunner ---

  private enum Action {
    case Succeed
    case Conflict
    case Fail(msg: String)
  }

  private class FakeJobRunner(jobs: Ref[Map[JobRunId, JobRun]], nextAction: Ref[Action]) extends JobRunner {

    override def submit(
      kind: JobKind,
      clubSlug: Option[ClubSlug],
      params: Option[String],
      trigger: RunTrigger,
      effect: RIO[CcasLogger & ChessComClient & Transactor, Any]
    ): RIO[Transactor, JobRunId] =
      nextAction.get.flatMap {
        case Action.Succeed =>
          val id  = JobRunId.generate()
          val now = Instant.now()
          val job = JobRun(id, kind, trigger, JobRunStatus.Running, clubSlug, params, now, None, None)
          jobs.update(_ + (id -> job)).as(id)
        case Action.Conflict =>
          ZIO.fail(new JobConflictException(s"A $kind job is already running"))
        case Action.Fail(msg) =>
          ZIO.fail(new Exception(msg))
      }

    override def status(id: JobRunId): RIO[Transactor, Option[JobRun]] =
      jobs.get.map(_.get(id))

    override def recentJobs(limit: Int): RIO[Transactor, List[JobRun]] =
      jobs.get.map(_.values.toList.sortBy(_.startedAt)(using Ordering[Instant].reverse).take(limit))

    def setNextAction(action: Action): UIO[Unit] = nextAction.set(action)

    def prePopulate(jobRun: JobRun): UIO[Unit] = jobs.update(_ + (jobRun.id -> jobRun))
  }

  private val fakeJobRunnerLayer: ZLayer[Any, Nothing, JobRunner] =
    ZLayer.fromZIO {
      for {
        jobs       <- Ref.make(Map.empty[JobRunId, JobRun])
        nextAction <- Ref.make[Action](Action.Succeed)
      } yield new FakeJobRunner(jobs, nextAction)
    }

  private val dummyChessComClientLayer: ZLayer[Any, Nothing, ChessComClient] =
    ZLayer.fromZIO {
      for {
        semaphore  <- Semaphore.make(1)
        stateRef   <- Ref.make(ChessComClient.ThrottleState(1, 0, Vector.empty))
        reserveRef  <- Ref.make(Chunk.empty[Fiber.Runtime[Nothing, Nothing]])
        adjustMutex <- Semaphore.make(1)
        activeRef   <- Ref.make(0)
        rateLimitGate <- Semaphore.make(1)
        lastReqRef  <- Ref.make(0L)
        ema         <- Ref.make(0.0)
        bar         <- TestCcasLogger.noopBar
      } yield {
        val routes: Routes[Any, Response] = Routes(
          Method.GET / trailing -> handler(Response(status = Status.NotFound))
        )
        val driver = new ZClient.Driver[Any, Scope, Throwable] {
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
            ZIO.die(new UnsupportedOperationException)
        }
        ChessComClient(
          ZClient.fromDriver(driver),
          Transactor(null),
          Headers.empty,
          TestCcasLogger.noop,
          semaphore,
          stateRef,
          reserveRef,
          adjustMutex,
          activeRef,
          rateLimitGate,
          lastReqRef,
          ema,
          bar,
          ChessComClient.ThrottleConfig(1, 30.seconds, 1.second, 5.seconds, 10.seconds, 20, 0.2, 10)
        )
      }
    }

  // --- Request helper ---

  private def jsonRequest(method: Method, path: String, body: String = ""): Request = {
    val url = URL.decode(path).toOption.get
    Request(
      method = method,
      url = url,
      body = if (body.isEmpty) { Body.empty }
      else { Body.fromString(body) }
    )
      .addHeader(Header.ContentType(MediaType.application.json))
  }

  private def getFakeRunner: URIO[JobRunner, FakeJobRunner] =
    ZIO.service[JobRunner].map(_.asInstanceOf[FakeJobRunner])

  // ==========================================================================
  // Suite: HealthRoutes
  // ==========================================================================

  private def suiteHealth = suite("HealthRoutes")(
    test("GET /health returns 200") {
      for {
        response <- HealthRoutes.routes.runZIO(jsonRequest(Method.GET, "/health"))
      } yield assertTrue(response.status == Status.Ok)
    },
    test("GET /health/ready returns 200 when DB is up") {
      for {
        response <- HealthRoutes.routes.runZIO(jsonRequest(Method.GET, "/health/ready"))
      } yield assertTrue(response.status == Status.Ok)
    }
  )

  // ==========================================================================
  // Suite: JobRoutes
  // ==========================================================================

  private def suiteJobRoutes = suite("JobRoutes")(
    test("POST /api/jobs/recruitment returns 202") {
      for {
        fake <- getFakeRunner
        _    <- fake.setNextAction(Action.Succeed)
        response <- JobRoutes.routes.runZIO(
          jsonRequest(Method.POST, "/api/jobs/recruitment", """{"clubSlug":"test-club"}""")
        )
      } yield assertTrue(response.status == Status.Accepted)
    },
    test("POST /api/jobs/recruitment returns 409 on conflict") {
      for {
        fake <- getFakeRunner
        _    <- fake.setNextAction(Action.Conflict)
        response <- JobRoutes.routes.runZIO(
          jsonRequest(Method.POST, "/api/jobs/recruitment", """{"clubSlug":"test-club"}""")
        )
      } yield assertTrue(response.status == Status.Conflict)
    },
    test("POST /api/jobs/recruitment returns 400 on bad JSON") {
      for {
        response <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/recruitment", "not json"))
      } yield assertTrue(response.status == Status.BadRequest)
    },
    test("POST /api/jobs/membership returns 202") {
      for {
        fake <- getFakeRunner
        _    <- fake.setNextAction(Action.Succeed)
        response <- JobRoutes.routes.runZIO(
          jsonRequest(Method.POST, "/api/jobs/membership", """{"clubSlug":"test-club"}""")
        )
      } yield assertTrue(response.status == Status.Accepted)
    },
    test("POST /api/jobs/matchref returns 202") {
      for {
        fake     <- getFakeRunner
        _        <- fake.setNextAction(Action.Succeed)
        response <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/matchref"))
      } yield assertTrue(response.status == Status.Accepted)
    },
    test("GET /api/jobs returns list") {
      val t0 = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
      val job = JobRun(
        JobRunId.wrap("list-id"),
        JobKind.Recruitment,
        RunTrigger.Cli,
        JobRunStatus.Completed,
        Some(ClubSlug("c")),
        None,
        t0,
        Some(t0),
        None
      )
      for {
        fake     <- getFakeRunner
        _        <- fake.prePopulate(job)
        response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs"))
        body     <- response.body.asString
      } yield assertTrue(
        response.status == Status.Ok,
        body.contains("list-id")
      )
    },
    test("GET /api/jobs/:id returns 200 for existing") {
      val t0  = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
      val job = JobRun(JobRunId.wrap("detail-id"), JobKind.Membership, RunTrigger.Cli, JobRunStatus.Running, None, None, t0, None, None)
      for {
        fake     <- getFakeRunner
        _        <- fake.prePopulate(job)
        response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/detail-id"))
        body     <- response.body.asString
      } yield assertTrue(
        response.status == Status.Ok,
        body.contains("detail-id"),
        body.contains("Membership")
      )
    },
    test("GET /api/jobs/:id returns 404 for unknown") {
      for {
        response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/nonexistent"))
      } yield assertTrue(response.status == Status.NotFound)
    }
  )

  // ==========================================================================
  // Suite: ScheduleRoutes
  // ==========================================================================

  private val deleteAllSchedules = connectZIO { val _ = sql"DELETE FROM job_schedule".update.run() }

  private def suiteScheduleRoutes = suite("ScheduleRoutes")(
    test("GET /api/schedules returns empty list") {
      for {
        _        <- deleteAllSchedules
        response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/schedules"))
        body     <- response.body.asString
      } yield assertTrue(
        response.status == Status.Ok,
        body == "[]"
      )
    },
    test("POST /api/schedules creates schedule") {
      for {
        _ <- deleteAllSchedules
        response <- ScheduleRoutes.routes.runZIO(
          jsonRequest(
            Method.POST,
            "/api/schedules",
            """{"kind":"Recruitment","clubSlug":"test-club","intervalHours":24}"""
          )
        )
        body <- response.body.asString
      } yield assertTrue(
        response.status == Status.Created,
        body.contains("Recruitment"),
        body.contains("test-club")
      )
    },
    test("GET /api/schedules returns created schedule") {
      for {
        response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/schedules"))
        body     <- response.body.asString
      } yield assertTrue(
        response.status == Status.Ok,
        body.contains("Recruitment")
      )
    },
    test("PUT /api/schedules/:id updates") {
      for {
        // Get existing schedule id
        listResp <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/schedules"))
        listBody <- listResp.body.asString
        id = extractFirstId(listBody)
        response <- ScheduleRoutes.routes.runZIO(
          jsonRequest(Method.PUT, s"/api/schedules/$id", """{"intervalHours":48,"enabled":false}""")
        )
        body <- response.body.asString
      } yield assertTrue(
        response.status == Status.Ok,
        body.contains("48"),
        body.contains("false")
      )
    },
    test("DELETE /api/schedules/:id removes") {
      for {
        listResp <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/schedules"))
        listBody <- listResp.body.asString
        id = extractFirstId(listBody)
        response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.DELETE, s"/api/schedules/$id"))
      } yield assertTrue(response.status == Status.NoContent)
    },
    test("POST /api/schedules with invalid kind returns 500") {
      for {
        response <- ScheduleRoutes.routes.runZIO(
          jsonRequest(Method.POST, "/api/schedules", """{"kind":"InvalidKind","intervalHours":24}""")
        )
      } yield assertTrue(response.status == Status.InternalServerError)
    }
  )

  /** Extract the first `"id"` value from a JSON array response. */
  private def extractFirstId(json: String): Long = {
    val pattern = """"id"\s*:\s*(\d+)""".r
    pattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(
      throw new RuntimeException(s"Could not extract id from: $json")
    )
  }
}
