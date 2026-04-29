package ccas.server.routes

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql

import ccas.utils.sql.PostgresClient
import zio.{LogLevel, RIO, Ref, Scope, UIO, ULayer, URIO, ZIO, ZLayer}
import zio.http.*
import zio.json.DecoderOps
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault, ZTestLogger}

import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.jobs.*
import ccas.server.routes.JobRoutes.{ClubJobResult, JobResult}
import ccas.server.ServerTables
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.errors.ConflictException
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO
import ccas.utils.ProgressDisplay

object TestRoutes extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRoutes")(
    suiteHealth,
    suiteJobRoutes,
    suiteScheduleRoutes
  ).provideShared(
    FreshSchemaLayer("test_routes", onInit = ServerTables.ensureTables),
    fakeJobRunnerLayer,
    TestChessComClientSupport.dummyLayer,
    ZTestLogger.default,
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
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      nextAction.get.flatMap {
        case Action.Succeed =>
          val id  = JobRunId.generate()
          val now = Instant.now()
          val job = JobRun(id, kind, clubId, trigger, JobRunStatus.Running, params, now, None, None)
          jobs.update(_ + (id -> job)).as(id)
        case Action.Conflict =>
          ZIO.fail(ConflictException(s"A $kind job is already running"))
        case Action.Fail(msg) =>
          ZIO.fail(new Exception(msg))
      }

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] =
      jobs.get.map(_.get(id))

    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] =
      jobs.get.map(_.values.toList.sortBy(_.startedAt)(using Ordering[Instant].reverse).take(limit))

    def setNextAction(action: Action): UIO[Unit] = nextAction.set(action)

    def prePopulate(jobRun: JobRun): UIO[Unit] = jobs.update(_ + (jobRun.id -> jobRun))
  }

  private val fakeJobRunnerLayer: ULayer[JobRunner] =
    ZLayer.fromZIO {
      for {
        jobs       <- Ref.make(Map.empty[JobRunId, JobRun])
        nextAction <- Ref.make[Action](Action.Succeed)
      } yield new FakeJobRunner(jobs, nextAction)
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
    testHealthReturns200,
    testHealthReadyReturns200
  )

  private def testHealthReturns200 = test("GET /health returns 200") {
    for {
      response <- HealthRoutes.routes.runZIO(jsonRequest(Method.GET, "/health"))
    } yield assertTrue(response.status == Status.Ok)
  }

  private def testHealthReadyReturns200 = test("GET /health/ready returns 200 when DB is up") {
    for {
      response <- HealthRoutes.routes.runZIO(jsonRequest(Method.GET, "/health/ready"))
    } yield assertTrue(response.status == Status.Ok)
  }

  // ==========================================================================
  // Suite: JobRoutes
  // ==========================================================================

  private def suiteJobRoutes = suite("JobRoutes")(
    testRecruitmentSuccess,
    testRecruitmentConflict,
    testRecruitmentBadJson,
    testMembershipSingleClub,
    testMembershipEmptyClubSlugs,
    testMembershipMultipleClubs,
    testMembershipWithUnknownClub,
    testHistorySingleClub,
    testMatchrefSuccess,
    testGetJobsReturnsList,
    testGetJobByIdReturns200,
    testGetJobByIdReturns404,
    testStatsWithInvalidDateReturns400,
    testStatsWithPartialDatesReturns400,
    testUnhandledErrorReturns500AndLogsCause,
    testInterruptPropagatesWithoutLogging
  )

  private def testRecruitmentSuccess = test("POST /api/jobs/recruitment success") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/recruitment", """{"clubSlug":"test-club"}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[JobResult]
    } yield assertTrue(
      response.status == Status.Ok,
      parsed.isRight,
      parsed.toOption.get.jobId.isDefined,
      parsed.toOption.get.error.isEmpty
    )
  }

  private def testRecruitmentConflict = test("POST /api/jobs/recruitment conflict") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Conflict)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/recruitment", """{"clubSlug":"test-club"}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[JobResult]
    } yield assertTrue(
      response.status == Status.Ok,
      parsed.isRight,
      parsed.toOption.get.jobId.isEmpty,
      parsed.toOption.get.error.exists(_.contains("already running"))
    )
  }

  private def testRecruitmentBadJson = test("POST /api/jobs/recruitment bad JSON") {
    for {
      response <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/recruitment", "not json"))
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testMembershipSingleClub = test("POST /api/jobs/membership single club") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/membership", """{"clubSlugs":["test-club"]}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[ClubJobResult]]
    } yield {
      val results = parsed.toOption.get
      assertTrue(
        response.status == Status.Ok,
        parsed.isRight,
        results.size == 1,
        results.head.clubSlug == "test-club",
        results.head.jobId.isDefined,
        results.head.error.isEmpty
      )
    }
  }

  private def testMembershipEmptyClubSlugs = test("POST /api/jobs/membership empty clubSlugs") {
    for {
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/membership", """{"clubSlugs":[]}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testMembershipMultipleClubs = test("POST /api/jobs/membership multiple clubs") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/membership", """{"clubSlugs":["test-club","other-club"]}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[ClubJobResult]]
    } yield {
      val results = parsed.toOption.get
      assertTrue(
        response.status == Status.Ok,
        parsed.isRight,
        results.size == 2,
        results.map(_.clubSlug).toSet == Set("test-club", "other-club"),
        results.forall(r => r.jobId.isDefined && r.error.isEmpty)
      )
    }
  }

  private def testMembershipWithUnknownClub = test("POST /api/jobs/membership with unknown club") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/membership", """{"clubSlugs":["test-club","no-such-club"]}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[ClubJobResult]]
    } yield {
      val results  = parsed.toOption.get
      val found    = results.find(_.clubSlug == "test-club").get
      val notFound = results.find(_.clubSlug == "no-such-club").get
      assertTrue(
        response.status == Status.Ok,
        parsed.isRight,
        results.size == 2,
        found.jobId.isDefined,
        found.error.isEmpty,
        notFound.jobId.isEmpty,
        notFound.error.contains("Club not found")
      )
    }
  }

  private def testHistorySingleClub = test("POST /api/jobs/history single club") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/history", """{"clubSlugs":["test-club"]}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[ClubJobResult]]
    } yield {
      val results = parsed.toOption.get
      assertTrue(
        response.status == Status.Ok,
        parsed.isRight,
        results.size == 1,
        results.head.clubSlug == "test-club",
        results.head.jobId.isDefined,
        results.head.error.isEmpty
      )
    }
  }

  private def testMatchrefSuccess = test("POST /api/jobs/matchref success") {
    for {
      fake     <- getFakeRunner
      _        <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/matchref"))
      body     <- response.body.asString
      parsed = body.fromJson[JobResult]
    } yield assertTrue(
      response.status == Status.Ok,
      parsed.isRight,
      parsed.toOption.get.jobId.isDefined,
      parsed.toOption.get.error.isEmpty
    )
  }

  private def testGetJobsReturnsList = test("GET /api/jobs returns list") {
    val t0 = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val job = JobRun(
      JobRunId.wrap("list-id"),
      JobKind.Recruitment,
      Some(ClubId(200)),
      RunTrigger.Cli,
      JobRunStatus.Completed,
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
  }

  private def testGetJobByIdReturns200 = test("GET /api/jobs/:id returns 200 for existing") {
    val t0 = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val job = JobRun(
      JobRunId.wrap("detail-id"),
      JobKind.Membership,
      None,
      RunTrigger.Cli,
      JobRunStatus.Running,
      None,
      t0,
      None,
      None
    )
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
  }

  private def testGetJobByIdReturns404 = test("GET /api/jobs/:id returns 404 for unknown") {
    for {
      response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/nonexistent"))
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testStatsWithInvalidDateReturns400 = test("POST /api/jobs/stats with invalid date returns 400") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/stats", """{"clubSlug":"test-club","since":"not-a-date","until":"also-bad"}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testStatsWithPartialDatesReturns400 = test("POST /api/jobs/stats with only 'since' returns 400") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/stats", """{"clubSlug":"test-club","since":"2026-01-01T00:00:00Z"}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testInterruptPropagatesWithoutLogging =
    test("interrupted effect propagates and is not logged as a 500") {
      val interrupted: zio.Task[Response] = ZIO.interrupt
      for {
        logsBefore <- ZTestLogger.logOutput.map(_.size)
        exit       <- RouteHelpers.withErrorHandling(interrupted).exit
        logsAfter  <- ZTestLogger.logOutput.map(_.size)
      } yield assertTrue(exit.isInterrupted, logsAfter == logsBefore)
    }

  private def testUnhandledErrorReturns500AndLogsCause =
    test("unhandled non-user-facing error returns generic 500 and logs cause") {
      val msg = "simulated downstream failure for test"
      for {
        _    <- ensureClubs
        fake <- getFakeRunner
        _    <- fake.setNextAction(Action.Fail(msg))
        response <- JobRoutes.routes.runZIO(
          jsonRequest(Method.POST, "/api/jobs/recruitment", """{"clubSlug":"test-club"}""")
        )
        body <- response.body.asString
        logs <- ZTestLogger.logOutput
      } yield assertTrue(
        response.status == Status.InternalServerError,
        body == """{"error":"Internal server error"}""",
        logs.exists(entry =>
          entry.logLevel == LogLevel.Error &&
            entry.cause.failures.exists {
              case t: Throwable => Option(t.getMessage).contains(msg)
              case _            => false
            }
        )
      )
    }

  // ==========================================================================
  // Suite: ScheduleRoutes
  // ==========================================================================

  private val t0 = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private val ensureClubs = for {
    _ <- Club.upsert(Club(ClubId(200), t0, ClubSlug("test-club"), "Test Club", None, None, None))
    _ <- Club.upsert(Club(ClubId(201), t0, ClubSlug("other-club"), "Other Club", None, None, None))
  } yield ()

  private val deleteAllSchedules = for {
    _ <- connectZIO { val _ = sql"DELETE FROM job_schedule".update.run() }
    _ <- ensureClubs
  } yield ()

  private def suiteScheduleRoutes = suite("ScheduleRoutes")(
    testGetSchedulesReturnsEmptyList,
    testPostSchedulesCreatesSchedule,
    testGetSchedulesReturnsCreatedSchedule,
    testPutScheduleUpdates,
    testDeleteScheduleRemoves,
    testPostSchedulesInvalidKindReturns400,
    testPostSchedulesNonPositiveIntervalReturns400,
    testPostSchedulesUnknownClubReturns404,
    testPutScheduleNonPositiveIntervalReturns400,
    testPutScheduleUnknownIdReturns404
  )

  private def testGetSchedulesReturnsEmptyList = test("GET /api/schedules returns empty list") {
    for {
      _        <- deleteAllSchedules
      response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/schedules"))
      body     <- response.body.asString
    } yield assertTrue(
      response.status == Status.Ok,
      body == "[]"
    )
  }

  private def testPostSchedulesCreatesSchedule = test("POST /api/schedules creates schedule") {
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
      body.contains("200") // clubId
    )
  }

  private def testGetSchedulesReturnsCreatedSchedule = test("GET /api/schedules returns created schedule") {
    for {
      response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/schedules"))
      body     <- response.body.asString
    } yield assertTrue(
      response.status == Status.Ok,
      body.contains("Recruitment")
    )
  }

  private def testPutScheduleUpdates = test("PUT /api/schedules/:id updates") {
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
  }

  private def testDeleteScheduleRemoves = test("DELETE /api/schedules/:id removes") {
    for {
      listResp <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/schedules"))
      listBody <- listResp.body.asString
      id = extractFirstId(listBody)
      response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.DELETE, s"/api/schedules/$id"))
    } yield assertTrue(response.status == Status.NoContent)
  }

  private def testPostSchedulesInvalidKindReturns400 = test("POST /api/schedules with invalid kind returns 400") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/schedules", """{"kind":"InvalidKind","intervalHours":24}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostSchedulesNonPositiveIntervalReturns400 = test("POST /api/schedules with non-positive intervalHours returns 400") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/schedules", """{"kind":"Recruitment","clubSlug":"test-club","intervalHours":0}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostSchedulesUnknownClubReturns404 = test("POST /api/schedules with unknown club returns 404") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/schedules", """{"kind":"Recruitment","clubSlug":"no-such-club","intervalHours":24}""")
      )
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testPutScheduleNonPositiveIntervalReturns400 = test("PUT /api/schedules/:id with non-positive intervalHours returns 400") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.PUT, "/api/schedules/1", """{"intervalHours":-1}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPutScheduleUnknownIdReturns404 = test("PUT /api/schedules with unknown id returns 404") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.PUT, "/api/schedules/999999", """{"enabled":false}""")
      )
    } yield assertTrue(response.status == Status.NotFound)
  }

  /** Extract the first `"id"` value from a JSON array response. */
  private def extractFirstId(json: String): Long = {
    val pattern = """"id"\s*:\s*(\d+)""".r
    pattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(
      throw new RuntimeException(s"Could not extract id from: $json")
    )
  }
}
