package ccas.server.routes

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql

import zio.{LogLevel, RIO, Ref, Scope, Task, UIO, ULayer, URIO, ZIO, ZLayer}
import zio.http.*
import zio.stream.ZStream
import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault, ZTestLogger}

import ccas.analysis.apps.{ClubQuery, ClubResolution, NamedClub}
import ccas.analysis.apps.recruitment.{CandidateOutcome, CriteriaSpec}
import ccas.analysis.tables.{
  Club,
  ManagedClub,
  Player,
  RecruitmentCandidate,
  RecruitmentCriteria,
  RecruitmentRun,
  RunTrigger
}
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId, PlayerId, Username}
import ccas.server.jobs.*
import ccas.server.routes.JobRoutes.{ClubJobResult, ConfirmResult, InvitedUsernames, JobResult}
import ccas.server.scheduler.{JobSchedule, ScheduleSeed}
import ccas.server.ServerTables
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.errors.ConflictException
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient, TestDbCleanup}
import ccas.utils.ProgressDisplay

object TestRoutes extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRoutes")(
    suiteHealth,
    suiteJobRoutes,
    suiteScheduleRoutes,
    suiteClubRoutes,
    suiteManagedClubRoutes,
    suiteRecruitmentCriteriaRoutes
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

  private class FakeJobRunner(
    jobs: Ref[Map[JobRunId, JobRun]],
    nextAction: Ref[Action],
    sweptLogs: Ref[Set[JobRunId]]
  ) extends JobRunner {

    override def submit(
      kind: JobKind,
      clubIdOption: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] =
      nextAction.get.flatMap {
        case Action.Succeed =>
          val id  = JobRunId.generate()
          val now = Instant.now()
          val job = JobRun(id, kind, clubIdOption, trigger, JobRunStatus.Running, params, now, None, None)
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

    // Canned two-line stream for any known job — enough to pin the route's 200/404/410 + framing without the real
    // file-tailing mechanics (those are covered against JobRunner.live in TestJobRunner).
    override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs] =
      for {
        known <- jobs.get.map(_.contains(id))
        swept <- sweptLogs.get.map(_.contains(id))
      } yield {
        if (!known) { JobLogs.NoSuchJob }
        else if (swept) { JobLogs.Expired }
        else { JobLogs.Streaming(ZStream.fromIterable(List("alpha", "beta"))) }
      }

    // Canned single progress frame for any known job; None for unknown — pins the /progress route's 200/404 + framing.
    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] =
      jobs.get.map(_.get(id).map(_ => ZStream.fromIterable(List("""{"bars":[]}"""))))

    override def sweepLogs: URIO[PostgresClient, Int] = ZIO.succeed(0)

    def setNextAction(action: Action): UIO[Unit] = nextAction.set(action)

    def prePopulate(jobRun: JobRun): UIO[Unit] = jobs.update(_ + (jobRun.id -> jobRun))

    def markLogSwept(id: JobRunId): UIO[Unit] = sweptLogs.update(_ + id)
  }

  private val fakeJobRunnerLayer: ULayer[JobRunner] =
    ZLayer.fromZIO {
      for {
        jobs       <- Ref.make(Map.empty[JobRunId, JobRun])
        nextAction <- Ref.make[Action](Action.Succeed)
        sweptLogs  <- Ref.make(Set.empty[JobRunId])
      } yield new FakeJobRunner(jobs, nextAction, sweptLogs)
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
    testMembershipResolvesByIdOverStaleSlug,
    testMembershipEmptyClubSlugs,
    testMembershipMultipleClubs,
    testMembershipWithUnknownClub,
    testMembershipByFormerSlug,
    testHistorySingleClub,
    testMatchrefSuccess,
    testGetJobsReturnsList,
    testGetJobByIdReturns200,
    testGetJobByIdReturns404,
    testCancelJobReturns200,
    testCancelJobReturns404,
    testGetJobLogsReturns200,
    testGetJobLogsReturns404,
    testGetJobLogsReturns410ForASweptLog,
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
        jsonRequest(Method.POST, "/api/jobs/recruitment", s"""{"club":$testClub}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[ClubJobResult]
    } yield assertTrue(
      response.status == Status.Ok,
      parsed.isRight,
      parsed.toOption.get.club == "test-club",
      parsed.toOption.get.jobIdOption.isDefined,
      parsed.toOption.get.error.isEmpty,
      parsed.toOption.get.resolution == ClubResolution.Known(NamedClub(ClubId(200), ClubSlug("test-club")))
    )
  }

  private def testRecruitmentConflict = test("POST /api/jobs/recruitment conflict") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Conflict)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/recruitment", s"""{"club":$testClub}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[ClubJobResult]
    } yield assertTrue(
      response.status == Status.Ok,
      parsed.isRight,
      parsed.toOption.get.jobIdOption.isEmpty,
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
        jsonRequest(Method.POST, "/api/jobs/membership", """{"clubs":[{"kind":"by_slug","slug":"test-club"}]}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[ClubJobResult]]
    } yield {
      val results = parsed.toOption.get
      assertTrue(
        response.status == Status.Ok,
        parsed.isRight,
        results.size == 1,
        results.head.club == "test-club",
        results.head.jobIdOption.isDefined,
        results.head.error.isEmpty
      )
    }
  }

  private def testMembershipEmptyClubSlugs = test("POST /api/jobs/membership empty clubSlugs") {
    for {
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/membership", """{"clubs":[]}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testMembershipMultipleClubs = test("POST /api/jobs/membership multiple clubs") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/membership", """{"clubs":[{"kind":"by_slug","slug":"test-club"},{"kind":"by_slug","slug":"other-club"}]}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[ClubJobResult]]
    } yield {
      val results = parsed.toOption.get
      assertTrue(
        response.status == Status.Ok,
        parsed.isRight,
        results.size == 2,
        results.map(_.club).toSet == Set("test-club", "other-club"),
        results.forall(r => r.jobIdOption.isDefined && r.error.isEmpty)
      )
    }
  }

  // Case B (#176): the CLI holds a stale slug for a club that has since been renamed, but the correct stable id. The
  // server must resolve by id and report the canonical slug back (for `current_club` refresh), never 404 on the slug.
  private def testMembershipResolvesByIdOverStaleSlug =
    test("POST /api/jobs/membership resolves by clubId even when the slug is stale") {
      for {
        _    <- ensureClubs
        fake <- getFakeRunner
        _    <- fake.setNextAction(Action.Succeed)
        // clubId 200 is "test-club"; the slug sent no longer exists, but the id pins the club.
        response <- JobRoutes.routes.runZIO(
          jsonRequest(Method.POST, "/api/jobs/membership", """{"clubs":[{"kind":"by_id","clubId":200}]}""")
        )
        body   <- response.body.asString
        parsed = body.fromJson[List[ClubJobResult]]
      } yield {
        val r = parsed.toOption.get.head
        assertTrue(
          response.status == Status.Ok,
          r.club == "test-club", // labelled by the club that ran, which is the point of sending an id
          r.jobIdOption.isDefined,
          r.error.isEmpty,
          // the canonical id and current slug, for current_club refresh
          r.resolution.runnable == Right(NamedClub(ClubId(200), ClubSlug("test-club")))
        )
      }
    }

  private def testMembershipWithUnknownClub = test("POST /api/jobs/membership with unknown club") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/membership", """{"clubs":[{"kind":"by_slug","slug":"test-club"},{"kind":"by_slug","slug":"no-such-club"}]}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[ClubJobResult]]
    } yield {
      val results  = parsed.toOption.get
      val found    = results.find(_.club == "test-club").get
      val notFound = results.find(_.club == "no-such-club").get
      assertTrue(
        response.status == Status.Ok,
        parsed.isRight,
        results.size == 2,
        found.jobIdOption.isDefined,
        found.error.isEmpty,
        found.resolution.runnable.isRight,
        notFound.jobIdOption.isEmpty,
        notFound.failure.exists(_.startsWith("Club not found")),
        notFound.resolution == ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("no-such-club")))
      )
    }
  }

  // #254: a slug the club used to hold still submits the job, against the current slug, flagged Renamed.
  private def testMembershipByFormerSlug =
    test("POST /api/jobs/membership by a former slug runs the job and reports Renamed with the current slug") {
      val club = Club(ClubId(210), t0, ClubSlug("route-current"), "Renamed Route Club", None, None, None)
      for {
        _    <- Club.upsert(club.copy(slug = ClubSlug("route-former")))
        _    <- Club.upsert(club)
        fake <- getFakeRunner
        _    <- fake.setNextAction(Action.Succeed)
        response <- JobRoutes.routes.runZIO(
          jsonRequest(Method.POST, "/api/jobs/membership", """{"clubs":[{"kind":"by_slug","slug":"route-former"}]}""")
        )
        body   <- response.body.asString
        parsed = body.fromJson[List[ClubJobResult]]
      } yield {
        val r = parsed.toOption.get.head
        assertTrue(
          response.status == Status.Ok,
          r.club == "route-current", // the club the job runs against; the resolution carries what was asked for
          r.jobIdOption.isDefined,
          r.error.isEmpty,
          r.resolution == ClubResolution.Renamed(NamedClub(club.clubId, club.slug), ClubSlug("route-former"))
        )
      }
    }

  private def testHistorySingleClub = test("POST /api/jobs/history single club") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/history", """{"clubs":[{"kind":"by_slug","slug":"test-club"}]}""")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[ClubJobResult]]
    } yield {
      val results = parsed.toOption.get
      assertTrue(
        response.status == Status.Ok,
        parsed.isRight,
        results.size == 1,
        results.head.club == "test-club",
        results.head.jobIdOption.isDefined,
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
      parsed.toOption.get.jobIdOption.isDefined,
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
      clubIdOption = None,
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
      clubIdOption = None,
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

  /** 410 and not 404: the job exists, only its log has aged out — a 404 would send the operator hunting for a typo.
    */
  private def testGetJobLogsReturns410ForASweptLog =
    test("GET /api/jobs/:id/logs returns 410 for a job whose log has aged out") {
      val t0 = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
      val job = JobRun(
        id = JobRunId.wrap("swept-id"),
        kind = JobKind.Membership,
        clubIdOption = None,
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
        _        <- fake.markLogSwept(job.id)
        response <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/jobs/swept-id/logs"))
        body     <- response.body.asString
      } yield assertTrue(response.status == Status.Gone, body.contains("no log available"))
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
        clubIdOption = None,
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
        jsonRequest(Method.POST, "/api/jobs/stats", s"""{"club":$testClub,"since":"not-a-date","until":"also-bad"}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testStatsWithPartialDatesReturns400 = test("POST /api/jobs/stats with only 'since' returns 400") {
    for {
      _    <- ensureClubs
      fake <- getFakeRunner
      _    <- fake.setNextAction(Action.Succeed)
      response <- JobRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/jobs/stats", s"""{"club":$testClub,"since":"2026-01-01T00:00:00Z"}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testInterruptPropagatesWithoutLogging =
    test("interrupted effect propagates and is not logged as a 500") {
      val interrupted: Task[Response] = ZIO.interrupt
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
          jsonRequest(Method.POST, "/api/jobs/recruitment", s"""{"club":$testClub}""")
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

  private val testClub = """{"kind":"by_slug","slug":"test-club"}"""

  private val ensureClubs = for {
    _ <- Club.upsert(Club(ClubId(200), t0, ClubSlug("test-club"), "Test Club", None, None, None))
    _ <- Club.upsert(Club(ClubId(201), t0, ClubSlug("other-club"), "Other Club", None, None, None))
  } yield ()

  // A club whose former name `renamed-from` only `club_name` still knows. Re-running it renames the club back and
  // forth, which leaves it the one club that ever held either name.
  private val renamedClubId = ClubId(210)
  private val ensureRenamedClub = for {
    _ <- Club.upsert(Club(renamedClubId, t0, ClubSlug("renamed-from"), "Renamed Club", None, None, None))
    _ <- Club.upsert(Club(renamedClubId, t0, ClubSlug("renamed-to"), "Renamed Club", None, None, None))
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
    Player.insertIfNew(Player(PlayerId(pid), t0, Username(name), PlayerStatusCategory.Active, None, t0)).unit

  private def testRecruitmentInvitedAndFound = test("GET recruitment invited/found split by outcome; 404 for unknown job") {
    for {
      _ <- seedRecruitmentRun(
        jobId = "rr-job-1",
        clubId = ClubId(200),
        invited = List((9001L, "alice")),
        deferred = List((9002L, "bob"))
      )
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
      runId <- seedRecruitmentRun(
        jobId = "rr-job-2",
        clubId = ClubId(200),
        invited = Nil,
        deferred = List((9101L, "carol"), (9102L, "dave"))
      )
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

  private def testRecruitmentReport = test("GET recruitment report by run id or club, by current or former name") {
    for {
      runId <- seedRecruitmentRun(
        jobId = "rr-job-3",
        clubId = ClubId(201),
        invited = List((9201L, "erin")),
        deferred = Nil
      )
      _       <- Club.upsert(Club(ClubId(202), t0, ClubSlug("empty-club"), "Empty Club", None, None, None))
      byRun   <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, s"/api/recruitment/runs/${RecruitmentRunId.unwrap(runId)}/invited"))
      byRunB  <- byRun.body.asString
      latest  <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/latest/invited?slug=other-club"))
      latestB <- latest.body.asString
      badId   <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/runs/not-a-number/invited"))
      noRun   <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/runs/999999/invited"))
      noClub  <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/latest/invited?slug=ghost-club"))
      noClubB <- noClub.body.asString
      noRuns  <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/latest/invited?slug=empty-club"))
      unnamed <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/latest/invited"))
      _       <- ensureRenamedClub
      unrun   <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/latest/invited?slug=renamed-from"))
      unrunB  <- unrun.body.asString
      _ <- seedRecruitmentRun(
        jobId = "rr-job-4",
        clubId = renamedClubId,
        invited = List((9202L, "fay")),
        deferred = Nil
      )
      former  <- JobRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/recruitment/latest/invited?slug=renamed-from"))
      formerB <- former.body.asString
    } yield assertTrue(
      byRun.status == Status.Ok,
      byRunB.fromJson[InvitedUsernames] == Right(InvitedUsernames(List("erin"))),
      latest.status == Status.Ok,
      latestB.fromJson[ClubResult[InvitedUsernames]].map(_.resultOption) == Right(Some(InvitedUsernames(List("erin")))),
      badId.status == Status.BadRequest,
      noRun.status == Status.NotFound,
      noClub.status == Status.Ok,
      noClubB.fromJson[ClubResult[InvitedUsernames]].map(_.resultOption) == Right(None),
      noRuns.status == Status.NotFound,
      unnamed.status == Status.BadRequest,
      // A failure past resolution carries no resolution, so it names the club the way it was asked for.
      unrun.status == Status.NotFound,
      unrunB.contains("renamed-from"),
      formerB.fromJson[ClubResult[InvitedUsernames]].map(_.resultOption) == Right(Some(InvitedUsernames(List("fay"))))
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
    testPostSchedulesUnknownClubCreatesNothing,
    testPostSchedulesValidatesBeforeResolving,
    testPostSchedulesByFormerName,
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
          s"""{"kind":"Recruitment","club":$testClub,"intervalHours":24}"""
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
        jsonRequest(Method.POST, "/api/schedules", s"""{"kind":"Recruitment","club":$testClub,"intervalHours":0}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostSchedulesUnknownClubCreatesNothing =
    test("POST /api/schedules for a club never ingested creates nothing, and answers with the resolution") {
      val body     = """{"kind":"Recruitment","club":{"kind":"by_slug","slug":"no-such-club"},"intervalHours":24}"""
      val notLocal = ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("no-such-club")))
      for {
        _        <- deleteAllSchedules
        response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/schedules", body))
        created  <- response.body.asString.map(_.fromJson[ScheduleRoutes.CreateScheduleResponse])
        all      <- JobSchedule.selectAll
      } yield assertTrue(
        response.status == Status.Ok,
        created.exists(_.scheduleOption.isEmpty),
        created.exists(_.resolutionOption.contains(notLocal)),
        all.isEmpty
      )
    }

  // Resolving may ask Chess.com and record what it answers, so a request that will be refused must not get that far.
  private def testPostSchedulesValidatesBeforeResolving =
    test("POST /api/schedules refuses a bad trigger before it resolves the club") {
      val body = """{"kind":"Recruitment","club":{"kind":"by_slug","slug":"no-such-club"},"intervalHours":0}"""
      for {
        response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/schedules", body))
      } yield assertTrue(response.status == Status.BadRequest)
    }

  private def testPostSchedulesByFormerName =
    test("POST /api/schedules by a former name schedules the club that holds it now (#254)") {
      val body = """{"kind":"Membership","club":{"kind":"by_slug","slug":"renamed-from"},"intervalHours":24}"""
      for {
        _        <- deleteAllSchedules
        _        <- ensureRenamedClub
        response <- ScheduleRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/schedules", body))
        all      <- JobSchedule.selectAll
      } yield assertTrue(response.status == Status.Created, all.map(_.clubIdOption) == List(Some(renamedClubId)))
    }

  private def testPutScheduleNonPositiveIntervalReturns400 = test("PUT /api/schedules/:id with non-positive intervalHours returns 400") {
    for {
      _ <- deleteAllSchedules
      createResp <- ScheduleRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/schedules", s"""{"kind":"Recruitment","club":$testClub,"intervalHours":24}""")
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
        jsonRequest(Method.POST, "/api/schedules", s"""{"kind":"Recruitment","club":$testClub,"intervalHours":40000}""")
      )
    } yield assertTrue(response.status == Status.BadRequest)
  }

  // ==========================================================================
  // Suite: RecruitmentCriteriaRoutes
  // ==========================================================================

  private def suiteRecruitmentCriteriaRoutes = suite("RecruitmentCriteriaRoutes")(
    testCriteriaAcrossNames,
    testCriteriaUnknownClub,
    testCriteriaValidatesBeforeResolving
  )

  private def testCriteriaValidatesBeforeResolving =
    test("criteria with a blank alias are refused before the club resolves") {
      val criteria = CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily).toJson
      val body     = s"""{"club":{"kind":"by_slug","slug":"no-such-club"},"alias":"  ","criteria":$criteria}"""
      for {
        resp <- RecruitmentCriteriaRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/recruitment-criteria", body))
      } yield assertTrue(resp.status == Status.BadRequest)
    }

  private def testCriteriaAcrossNames =
    test("criteria set under a former name read back by id and by the current name (#254)") {
      val criteria = CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily).toJson
      val club     = """{"kind":"by_slug","slug":"renamed-from"}"""
      val body     = s"""{"club":$club,"alias":"across-names","criteria":$criteria}"""
      val renamed  = ClubResolution.Renamed(NamedClub(renamedClubId, ClubSlug("renamed-to")), ClubSlug("renamed-from"))
      for {
        _    <- ensureRenamedClub
        set  <- RecruitmentCriteriaRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/recruitment-criteria", body))
        show <- RecruitmentCriteriaRoutes.routes.runZIO(
          jsonRequest(Method.GET, s"/api/recruitment-criteria/across-names?clubId=$renamedClubId")
        )
        list <- RecruitmentCriteriaRoutes.routes.runZIO(
          jsonRequest(Method.GET, "/api/recruitment-criteria?slug=renamed-to")
        )
        setResult  <- set.body.asString.map(_.fromJson[ClubResult[RecruitmentCriteriaRoutes.SetCriteriaResponse]])
        showResult <- show.body.asString.map(_.fromJson[ClubResult[CriteriaSpec]])
        listResult <- list.body.asString.map(_.fromJson[ClubResult[List[RecruitmentCriteriaRoutes.AliasSummary]]])
      } yield assertTrue(
        setResult.exists(r => r.resolution == renamed && r.resultOption.isDefined),
        showResult.exists(_.resultOption.contains(CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily.capped))),
        listResult.exists(_.resultOption.exists(_.map(_.alias) == List("across-names")))
      )
    }

  private def testCriteriaUnknownClub = test("criteria for a club never ingested answer with no result") {
    for {
      resp <- RecruitmentCriteriaRoutes.routes.runZIO(
        jsonRequest(Method.GET, "/api/recruitment-criteria?slug=no-such-club")
      )
      body <- resp.body.asString
    } yield assertTrue(
      resp.status == Status.Ok,
      body.fromJson[ClubResult[List[RecruitmentCriteriaRoutes.AliasSummary]]].map(_.resultOption) == Right(None)
    )
  }

  // ==========================================================================
  // Suite: ClubRoutes
  // ==========================================================================

  private def suiteClubRoutes = suite("ClubRoutes")(
    testClubsListsNamedSorted,
    testClubsResponseWireShape
  )

  // ==========================================================================
  // Suite: ManagedClubRoutes
  // ==========================================================================

  private val resetManaged =
    PostgresClient.connectZIO(sql"DELETE FROM managed_club".update.run()) *> ensureClubs

  private val markTestClub = s"""{"club":$testClub}"""
  private val noSuchClub   = """{"club":{"kind":"by_slug","slug":"no-such-club"}}"""

  private def suiteManagedClubRoutes = suite("ManagedClubRoutes")(
    testMarkAndList,
    testMarkUnknownClub,
    testMarkByFormerName,
    testUnmarkRemoves,
    testUnmarkUnknownClub,
    testUnmarkClearsSchedules,
    testManagedListWireShape
  )

  private def testMarkAndList = test("POST marks a club; GET lists it") {
    for {
      _    <- resetManaged
      mark <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", markTestClub))
      list <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/managed-clubs"))
      body <- list.body.asString
      parsed = body.fromJson[List[ManagedClubRoutes.ManagedClubResponse]]
    } yield assertTrue(
      mark.status == Status.Ok,
      list.status == Status.Ok,
      parsed.toOption.exists(_.exists(_.slug == "test-club"))
    )
  }

  private def testMarkUnknownClub = test("POST for a club never ingested marks nothing") {
    for {
      _    <- resetManaged
      resp <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", noSuchClub))
      body <- resp.body.asString
      list <- ManagedClub.selectAllWithClub
    } yield assertTrue(
      resp.status == Status.Ok,
      body.fromJson[ClubResult[Boolean]].map(_.resultOption) == Right(None),
      list.isEmpty
    )
  }

  private def testUnmarkRemoves = test("DELETE clears the marker, by id or by name, and says whether there was one") {
    for {
      _   <- resetManaged
      _   <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", markTestClub))
      del     <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.DELETE, "/api/managed-clubs?clubId=200"))
      delBody <- del.body.asString
      again   <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.DELETE, "/api/managed-clubs?slug=test-club"))
      agBody  <- again.body.asString
      list    <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/managed-clubs"))
      body    <- list.body.asString
      parsed = body.fromJson[List[ManagedClubRoutes.ManagedClubResponse]]
    } yield assertTrue(
      del.status == Status.Ok,
      delBody.fromJson[ClubResult[Boolean]].map(_.resultOption) == Right(Some(true)),
      agBody.fromJson[ClubResult[Boolean]].map(_.resultOption) == Right(Some(false)),
      parsed.toOption.exists(_.isEmpty)
    )
  }

  private def testUnmarkUnknownClub = test("DELETE for a club never ingested unmarks nothing") {
    for {
      _    <- resetManaged
      resp <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.DELETE, "/api/managed-clubs?slug=no-such-club"))
      body <- resp.body.asString
    } yield assertTrue(
      resp.status == Status.Ok,
      body.fromJson[ClubResult[Boolean]].map(_.resultOption) == Right(None)
    )
  }

  private def testUnmarkClearsSchedules =
    test("DELETE also clears the club's per-club job_schedule rows, leaving other clubs' rows (#106)") {
      for {
        _   <- resetManaged
        _   <- TestDbCleanup.clearJobSchedules
        _   <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", markTestClub))
        _   <- JobSchedule.seedPerClubIfAbsent(ClubId(200), ScheduleSeed(JobKind.History, 24, enabled = true))
        _   <- JobSchedule.seedPerClubIfAbsent(ClubId(200), ScheduleSeed(JobKind.Membership, 24, enabled = true))
        // club 201 is never managed — proves deleteByClub keys on club_id, not managed status (peer isolation).
        _   <- JobSchedule.seedPerClubIfAbsent(ClubId(201), ScheduleSeed(JobKind.History, 24, enabled = true))
        del <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.DELETE, "/api/managed-clubs?slug=test-club"))
        all <- JobSchedule.selectAll
      } yield assertTrue(
        del.status == Status.Ok,
        !all.exists(_.clubIdOption.contains(ClubId(200))),
        all.exists(_.clubIdOption.contains(ClubId(201)))
      )
    }

  private def testMarkByFormerName = test("POST by a former name marks the club that holds it now (#254)") {
    val body = """{"club":{"kind":"by_slug","slug":"renamed-from"}}"""
    for {
      _    <- resetManaged
      _    <- ensureRenamedClub
      resp <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", body))
      list <- ManagedClub.selectAllWithClub
    } yield assertTrue(resp.status == Status.Ok, list.map(_.clubId) == List(renamedClubId))
  }

  private def testManagedListWireShape = test("GET response uses {clubId,slug,name,markedAt} wire shape") {
    for {
      _    <- resetManaged
      _    <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.POST, "/api/managed-clubs", markTestClub))
      resp <- ManagedClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/managed-clubs"))
      body <- resp.body.asString
    } yield assertTrue(
      body.contains("\"clubId\""),
      body.contains("\"slug\""),
      body.contains("\"name\""),
      body.contains("\"markedAt\"")
    )
  }

  private def testClubsListsNamedSorted =
    test("GET /api/clubs lists the clubs that hold a name, sorted by slug") {
      for {
        _ <- ensureClubs
        // 203 takes 202's name, so 202 holds none and is not offered — what excluding `_stale_` rows used to do.
        _        <- Club.upsert(Club(ClubId(202), t0, ClubSlug("contested-club"), "Losing Club", None, None, None))
        _        <- Club.upsert(Club(ClubId(203), t0, ClubSlug("contested-club"), "Holding Club", None, None, None))
        response <- ClubRoutes.routes.runZIO(jsonRequest(Method.GET, "/api/clubs"))
        body     <- response.body.asString
        // Drop the fixtures so they don't leak into the shared DB for any later suite.
        _ <- TestDbCleanup.deleteClub(ClubId(202))
        _ <- TestDbCleanup.deleteClub(ClubId(203))
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
          slugs.count(_ == "contested-club") == 1,
          clubs.find(_.slug.value == "contested-club").exists(_.name == "Holding Club"),
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
