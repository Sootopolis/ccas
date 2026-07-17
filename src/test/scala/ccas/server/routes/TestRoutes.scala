package ccas.server.routes

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql

import ccas.utils.sql.PostgresClient
import zio.{LogLevel, RIO, Ref, Scope, UIO, ULayer, URIO, ZIO, ZLayer}
import zio.http.*
import zio.stream.ZStream
import zio.json.DecoderOps
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault, ZTestLogger}

import ccas.analysis.apps.recruitment.CandidateOutcome
import ccas.analysis.tables.{Club, RecruitmentCandidate, RecruitmentCriteria, RecruitmentRun, RunTrigger}
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId, PlayerId, Username}
import ccas.server.jobs.*
import ccas.server.routes.JobRoutes.{ClubJobResult, ConfirmResult, InvitedUsernames, JobResult}
import ccas.server.scheduler.{JobSchedule, ScheduleSeed}
import ccas.server.ServerTables
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.errors.ConflictException
import ccas.utils.sql.{FreshSchemaLayer, TestDbCleanup}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.ProgressDisplay

object TestRoutes extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRoutes")(
    suiteHealth,
    suiteJobRoutes,
    suiteScheduleRoutes,
    suiteClubRoutes,
    suiteManagedClubRoutes
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

    // Flip a stored Running job to Cancelled (returns true); unknown or already-terminal → false. Enough to pin the
    // cancel route's 200/404 without the real fiber-interrupt mechanics (covered against JobRunner.live in TestJobRunner).
    override def cancel(id: JobRunId): UIO[Boolean] =
      jobs.modify { m =>
        m.get(id) match {
          case Some(job) if job.status == JobRunStatus.Running =>
            (true, m + (id -> job.copy(status = JobRunStatus.Cancelled, completedAt = Some(Instant.now()))))
          case _ => (false, m)
        }
      }

    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] =
      jobs.get.map(_.values.toList.sortBy(_.startedAt)(using Ordering[Instant].reverse).take(limit))

    // Canned two-line stream for any known job; None for unknown — enough to pin the route's 200/404 + framing
    // without the real file-tailing mechanics (those are covered against JobRunner.live in TestJobRunner).
    override def logStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
      jobs.get.map(_.get(id).map(_ => ZStream.fromIterable(List("alpha", "beta"))))

    // Canned single progress frame for any known job; None for unknown — pins the /progress route's 200/404 + framing.
    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
      jobs.get.map(_.get(id).map(_ => ZStream.fromIterable(List("""{"bars":[]}"""))))

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
    testCancelJobReturns200,
    testCancelJobReturns404,
    testGetJobLogsReturns200,
    testGetJobLogsReturns404,
    testGetJobProgressReturns200,
    testGetJobProgressReturns404,
    testStatsWithInvalidDateReturns400,
    testStatsWithPartialDatesReturns400,
    testRecruitmentInvitedAndFound,
    testRecruitmentConfirmFlipsDeferred,
    testRecruitmentReport,
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

  private def testCancelJobReturns200 = test("POST /api/jobs/:id/cancel returns 200 and cancels a running job") {
    val t0 = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val job = JobRun(
      id = JobRunId.wrap("cancel-id"),
      kind = JobKind.Membership,
      clubId = None,
      trigger = RunTrigger.Cli,
      status = JobRunStatus.Running,
      params = None,
      startedAt = t0,
      completedAt = None,
      error = None
    )
    for {
      fake     <- getFakeRunner
      _        <- fake.prePopulate(job)
      response <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/cancel-id/cancel"))
      body     <- response.body.asString
      after    <- fake.status(JobRunId.wrap("cancel-id"))
    } yield assertTrue(
      response.status == Status.Ok,
      body.contains("cancel-id"),
      after.exists(_.status == JobRunStatus.Cancelled)
    )
  }

  private def testCancelJobReturns404 = test("POST /api/jobs/:id/cancel returns 404 for an unknown or terminal job") {
    for {
      response <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/nonexistent/cancel"))
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testGetJobLogsReturns200 = test("GET /api/jobs/:id/logs streams chunked text/plain for an existing job") {
    val t0 = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val job = JobRun(
      id = JobRunId.wrap("logs-id"),
      kind = JobKind.Membership,
      clubId = None,
      trigger = RunTrigger.Cli,
      status = JobRunStatus.Completed,
      params = None,
      startedAt = t0,
      completedAt = Some(t0),
      error = None
    )
    for {
      fake     <- getFakeRunner
      _        <- fake.prePopulate(job)
      response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/logs-id/logs"))
      body     <- response.body.asString
    } yield assertTrue(
      response.status == Status.Ok,
      response.header(Header.ContentType).exists(_.mediaType == MediaType.text.`plain`),
      body == "alpha\nbeta\n"
    )
  }

  private def testGetJobLogsReturns404 = test("GET /api/jobs/:id/logs returns 404 plain text for unknown job") {
    for {
      response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/nonexistent/logs"))
      body     <- response.body.asString
    } yield assertTrue(
      response.status == Status.NotFound,
      body.contains("not found")
    )
  }

  private def testGetJobProgressReturns200 =
    test("GET /api/jobs/:id/progress streams chunked NDJSON for an existing job") {
      val t0 = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
      val job = JobRun(
        id = JobRunId.wrap("progress-id"),
        kind = JobKind.Membership,
        clubId = None,
        trigger = RunTrigger.Cli,
        status = JobRunStatus.Completed,
        params = None,
        startedAt = t0,
        completedAt = Some(t0),
        error = None
      )
      for {
        fake     <- getFakeRunner
        _        <- fake.prePopulate(job)
        response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/progress-id/progress"))
        body     <- response.body.asString
      } yield assertTrue(
        response.status == Status.Ok,
        response.header(Header.ContentType).exists(_.mediaType == MediaType.text.`plain`),
        body == "{\"bars\":[]}\n"
      )
    }

  private def testGetJobProgressReturns404 =
    test("GET /api/jobs/:id/progress returns 404 plain text for unknown job") {
      for {
        response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/nonexistent/progress"))
        body     <- response.body.asString
      } yield assertTrue(
        response.status == Status.NotFound,
        body.contains("not found")
      )
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

  // Seed a completed recruitment run linked to `jobId` with the given invited/deferred candidates (each a
  // (playerId, username) pair). Distinct ids/jobIds across tests avoid PK collisions on the shared schema.
  private def seedRecruitmentRun(
    jobId: String,
    clubId: ClubId,
    invited: List[(Long, String)],
    deferred: List[(Long, String)]
  ): RIO[PostgresClient, RecruitmentRunId] =
    for {
      _          <- ensureClubs
      criteriaId <- RecruitmentCriteria.insert(RecruitmentCriteria.defaultDaily)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Api, t0, None, Some(JobRunId.wrap(jobId)))
      _          <- ZIO.foreachDiscard(invited ++ deferred) { case (pid, name) => seedPlayer(pid, name) }
      _ <- ZIO.foreachDiscard(invited) { case (pid, _) =>
        RecruitmentCandidate.insert(RecruitmentCandidate(runId, PlayerId(pid), t0, CandidateOutcome.Invited, None))
      }
      _ <- ZIO.foreachDiscard(deferred) { case (pid, _) =>
        RecruitmentCandidate.insert(RecruitmentCandidate(runId, PlayerId(pid), t0, CandidateOutcome.Deferred, None))
      }
    } yield runId

  private def seedPlayer(pid: Long, name: String): RIO[PostgresClient, Unit] =
    PostgresClient.connectZIO {
      sql"""INSERT INTO player (player_id, joined, username, status, title, since)
            VALUES (${PlayerId(pid)}, $t0, ${Username(name)}, 'Active', NULL, $t0)
            ON CONFLICT (player_id) DO NOTHING""".update.run()
    }.unit

  private def testRecruitmentInvitedAndFound = test("GET recruitment invited/found split by outcome; 404 for unknown job") {
    for {
      _         <- seedRecruitmentRun("rr-job-1", ClubId(200), invited = List((9001L, "alice")), deferred = List((9002L, "bob")))
      invResp   <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/rr-job-1/recruitment/invited"))
      invBody   <- invResp.body.asString
      foundResp <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/rr-job-1/recruitment/found"))
      foundBody <- foundResp.body.asString
      missResp  <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/no-such-job/recruitment/found"))
    } yield assertTrue(
      invResp.status == Status.Ok,
      invBody.fromJson[InvitedUsernames] == Right(InvitedUsernames(List("alice"))),
      foundResp.status == Status.Ok,
      foundBody.fromJson[InvitedUsernames] == Right(InvitedUsernames(List("bob"))),
      missResp.status == Status.NotFound
    )
  }

  private def testRecruitmentConfirmFlipsDeferred = test("POST recruitment confirm flips Deferred, records count, idempotent, 404 unknown") {
    for {
      runId    <- seedRecruitmentRun("rr-job-2", ClubId(200), invited = Nil, deferred = List((9101L, "carol"), (9102L, "dave")))
      resp1    <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/rr-job-2/recruitment/confirm"))
      body1    <- resp1.body.asString
      runAfter <- RecruitmentRun.selectId(runId)
      resp2    <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/rr-job-2/recruitment/confirm"))
      body2    <- resp2.body.asString
      missResp <- JobRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/jobs/no-such-job/recruitment/confirm"))
    } yield assertTrue(
      resp1.status == Status.Ok,
      body1.fromJson[ConfirmResult] == Right(ConfirmResult(2, List("carol", "dave"))),
      runAfter.exists(_.candidatesFound == 2),
      // Re-POST: nothing left to flip; count stays 2 and the same invited list comes back.
      body2.fromJson[ConfirmResult] == Right(ConfirmResult(0, List("carol", "dave"))),
      missResp.status == Status.NotFound
    )
  }

  private def testRecruitmentReport = test("GET recruitment report by run id and club-latest; 400/404 branches") {
    for {
      runId   <- seedRecruitmentRun("rr-job-3", ClubId(201), invited = List((9201L, "erin")), deferred = Nil)
      _       <- Club.upsert(Club(ClubId(202), t0, ClubSlug("empty-club"), "Empty Club", None, None, None))
      byRun   <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, s"/api/recruitment/runs/${RecruitmentRunId.unwrap(runId)}/invited"))
      byRunB  <- byRun.body.asString
      latest  <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/clubs/other-club/latest/invited"))
      latestB <- latest.body.asString
      badId   <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/runs/not-a-number/invited"))
      noRun   <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/runs/999999/invited"))
      noClub  <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/clubs/ghost-club/latest/invited"))
      noRuns  <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/clubs/empty-club/latest/invited"))
    } yield assertTrue(
      byRun.status == Status.Ok,
      byRunB.fromJson[InvitedUsernames] == Right(InvitedUsernames(List("erin"))),
      latest.status == Status.Ok,
      latestB.fromJson[InvitedUsernames] == Right(InvitedUsernames(List("erin"))),
      badId.status == Status.BadRequest,
      noRun.status == Status.NotFound,
      noClub.status == Status.NotFound,
      noRuns.status == Status.NotFound
    )
  }

  private val deleteAllSchedules = TestDbCleanup.clearJobSchedules *> ensureClubs

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
    testPutScheduleUnknownIdReturns404,
    testPostCronScheduleCreates,
    testPostCronInvalidExprReturns400,
    testPostCronInvalidTimezoneReturns400,
    testPostBothTriggersReturns400,
    testPostCronMissingExprReturns400,
    testPostSchedulesIntervalOverflowReturns400
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
      _ <- deleteAllSchedules
      createResp <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/schedules", """{"kind":"Recruitment","clubSlug":"test-club","intervalHours":24}""")
      )
      listResp <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/schedules"))
      listBody <- listResp.body.asString
      id = extractFirstId(listBody)
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.PUT, s"/api/schedules/$id", """{"intervalHours":-1}""")
      )
    } yield assertTrue(createResp.status == Status.Created, response.status == Status.BadRequest)
  }

  private def testPutScheduleUnknownIdReturns404 = test("PUT /api/schedules with unknown id returns 404") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.PUT, "/api/schedules/999999", """{"enabled":false}""")
      )
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testPostCronScheduleCreates = test("POST /api/schedules creates a cron schedule") {
    for {
      _ <- deleteAllSchedules
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(
          Method.POST,
          "/api/schedules",
          """{"kind":"ClubData","triggerType":"cron","cron":"0 9 * * MON","timezone":"Europe/London","misfire":"catch_up"}"""
        )
      )
      body <- response.body.asString
    } yield assertTrue(
      response.status == Status.Created,
      body.contains("\"triggerType\":\"cron\""),
      body.contains("Europe/London"),
      body.contains("catch_up"),
      body.contains("0 9 * * MON") // de-normalized back to the 5-field input the user typed (seconds dropped, ? -> *)
    )
  }

  private def testPostCronInvalidExprReturns400 = test("POST /api/schedules with an invalid cron returns 400") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/schedules", """{"kind":"ClubData","triggerType":"cron","cron":"99 9 * * *"}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostCronInvalidTimezoneReturns400 = test("POST /api/schedules with a bad timezone returns 400") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(
          Method.POST,
          "/api/schedules",
          """{"kind":"ClubData","triggerType":"cron","cron":"0 9 * * *","timezone":"Mars/Phobos"}"""
        )
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostBothTriggersReturns400 = test("POST /api/schedules with both intervalHours and cron returns 400") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(
          Method.POST,
          "/api/schedules",
          """{"kind":"ClubData","triggerType":"cron","intervalHours":24,"cron":"0 9 * * *"}"""
        )
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostCronMissingExprReturns400 = test("POST /api/schedules cron trigger without a cron expression returns 400") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/schedules", """{"kind":"ClubData","triggerType":"cron","timezone":"UTC"}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostSchedulesIntervalOverflowReturns400 = test("POST /api/schedules with intervalHours above SMALLINT returns 400") {
    for {
      response <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/schedules", """{"kind":"Recruitment","clubSlug":"test-club","intervalHours":40000}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  // ==========================================================================
  // Suite: ClubRoutes
  // ==========================================================================

  private def suiteClubRoutes = suite("ClubRoutes")(
    testClubsListsNonTombstonedSorted,
    testClubsResponseWireShape
  )

  // ==========================================================================
  // Suite: ManagedClubRoutes
  // ==========================================================================

  private val resetManaged =
    PostgresClient.connectZIO(sql"DELETE FROM managed_club".update.run()) *> ensureClubs

  private def suiteManagedClubRoutes = suite("ManagedClubRoutes")(
    testMarkAndList,
    testMarkUnknownClub404,
    testUnmarkRemoves,
    testUnmarkUnknownClub404,
    testUnmarkClearsSchedules,
    testManagedListWireShape
  )

  private def testMarkAndList = test("POST marks a club; GET lists it") {
    for {
      _    <- resetManaged
      mark <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", """{"clubSlug":"test-club"}"""))
      list <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/managed-clubs"))
      body <- list.body.asString
      parsed = body.fromJson[List[ManagedClubRoutes.ManagedClubResponse]]
    } yield assertTrue(
      mark.status == Status.Ok,
      list.status == Status.Ok,
      parsed.toOption.exists(_.exists(_.slug == "test-club"))
    )
  }

  private def testMarkUnknownClub404 = test("POST with unknown club returns 404") {
    for {
      _    <- resetManaged
      resp <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", """{"clubSlug":"no-such-club"}"""))
    } yield assertTrue(resp.status == Status.NotFound)
  }

  private def testUnmarkRemoves = test("DELETE clears the marker") {
    for {
      _   <- resetManaged
      _   <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", """{"clubSlug":"test-club"}"""))
      del <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.DELETE, "/api/managed-clubs/test-club"))
      list <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/managed-clubs"))
      body <- list.body.asString
      parsed = body.fromJson[List[ManagedClubRoutes.ManagedClubResponse]]
    } yield assertTrue(
      del.status == Status.NoContent,
      parsed.toOption.exists(_.isEmpty)
    )
  }

  // Unknown slug must still 404 through the new withTransaction wrapper (NotFoundException survives rollback).
  private def testUnmarkUnknownClub404 = test("DELETE with unknown club returns 404") {
    for {
      _    <- resetManaged
      resp <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.DELETE, "/api/managed-clubs/no-such-club"))
    } yield assertTrue(resp.status == Status.NotFound)
  }

  private def testUnmarkClearsSchedules =
    test("DELETE also clears the club's per-club job_schedule rows, leaving other clubs' rows (#106)") {
      for {
        _   <- resetManaged
        _   <- TestDbCleanup.clearJobSchedules
        _   <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", """{"clubSlug":"test-club"}"""))
        _   <- JobSchedule.seedPerClubIfAbsent(ClubId(200), ScheduleSeed(JobKind.History, 24, enabled = true))
        _   <- JobSchedule.seedPerClubIfAbsent(ClubId(200), ScheduleSeed(JobKind.Membership, 24, enabled = true))
        // club 201 is never managed — proves deleteByClub keys on club_id, not managed status (peer isolation).
        _   <- JobSchedule.seedPerClubIfAbsent(ClubId(201), ScheduleSeed(JobKind.History, 24, enabled = true))
        del <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.DELETE, "/api/managed-clubs/test-club"))
        all <- JobSchedule.selectAll
      } yield assertTrue(
        del.status == Status.NoContent,
        !all.exists(_.clubId.contains(ClubId(200))),
        all.exists(_.clubId.contains(ClubId(201)))
      )
    }

  private def testManagedListWireShape = test("GET response uses {slug,name,markedAt} wire shape") {
    for {
      _    <- resetManaged
      _    <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", """{"clubSlug":"test-club"}"""))
      resp <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/managed-clubs"))
      body <- resp.body.asString
    } yield assertTrue(
      body.contains("\"slug\""),
      body.contains("\"name\""),
      body.contains("\"markedAt\"")
    )
  }

  private def testClubsListsNonTombstonedSorted =
    test("GET /api/clubs lists non-tombstoned clubs sorted by slug, excluding _stale_ rows") {
      for {
        _        <- ensureClubs
        _        <- Club.upsert(Club(ClubId(202), t0, ClubSlug("_stale_202"), "Stale Club", None, None, None))
        response <- ClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/clubs"))
        body     <- response.body.asString
        // Drop the tombstone fixture so it doesn't leak into the shared DB for any later suite.
        _ <- PostgresClient.connectZIO(sql"DELETE FROM club WHERE club_id = 202".update.run())
        parsed = body.fromJson[ClubRoutes.ClubsResponse]
      } yield {
        val clubs = parsed.toOption.get.clubs
        val slugs = clubs.map(_.slug.value)
        assertTrue(
          response.status == Status.Ok,
          parsed.isRight,
          slugs == slugs.sorted,
          slugs.contains("other-club"),
          slugs.contains("test-club"),
          !slugs.contains("_stale_202"),
          clubs.find(_.slug.value == "test-club").exists(_.name == "Test Club")
        )
      }
    }

  private def testClubsResponseWireShape =
    test("GET /api/clubs response uses {clubs:[{slug,name}]} wire shape") {
      for {
        _        <- ensureClubs
        response <- ClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/clubs"))
        body     <- response.body.asString
      } yield assertTrue(
        response.status == Status.Ok,
        body.contains("\"clubs\""),
        body.contains("\"slug\""),
        body.contains("\"name\"")
      )
    }

  /** Extract the first `"id"` value from a JSON array response. */
  private def extractFirstId(json: String): Long = {
    val pattern = """"id"\s*:\s*(\d+)""".r
    pattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(
      throw new RuntimeException(s"Could not extract id from: $json")
    )
  }
}
