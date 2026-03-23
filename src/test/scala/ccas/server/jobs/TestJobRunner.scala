package ccas.server.jobs

import com.augustnagro.magnum.sql
import zio.{durationInt, Ref, Scope, Semaphore, Trace, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.ClubUrlName
import ccas.server.ServerTables
import ccas.utils.client.ChessComClient
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.SqlZioTypes.connectZIO

object TestJobRunner extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobRunner")(
    testSubmitSucceeds,
    testSubmitRecordsFailed,
    testSubmitRejectsDuplicate,
    testSubmitAllowsDifferentClub,
    testSubmitAllowsDifferentKind,
    testStatusUnknown,
    testRecentJobsOrdered
  ).provideShared(
    FreshSchemaLayer("test_job_runner", onInit = ServerTables.ensureTables),
    dummyChessComClientLayer,
    JobRunner.live
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)

  private val deleteAllJobRuns = connectZIO { val _ = sql"DELETE FROM job_run".update.run() }

  private val dummyChessComClientLayer: ZLayer[Any, Nothing, ChessComClient] =
    ZLayer.fromZIO {
      for {
        semaphore <- Semaphore.make(1)
        mutex     <- Semaphore.make(1)
        throttled <- Ref.make(false)
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
          Headers.empty,
          semaphore,
          mutex,
          throttled,
          zio.Duration.fromSeconds(30)
        )
      }
    }

  private def awaitStatus(
    runner: JobRunner,
    id: JobRunId,
    maxWait: zio.Duration = 10.seconds
  ): ZIO[com.augustnagro.magnum.Transactor, Throwable, JobRun] =
    runner.status(id).flatMap {
      case Some(job) if job.status != JobRunStatus.Running => ZIO.succeed(job)
      case _                                               => ZIO.sleep(100.millis) *> awaitStatus(runner, id, maxWait)
    }.timeoutFail(new Exception(s"Job $id did not complete in time"))(maxWait)

  // --- Tests ---

  private def testSubmitSucceeds = test("submit succeeds and completes") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id     <- runner.submit(JobKind.Recruitment, Some(ClubUrlName("test-club")), None, RunTrigger.Cli, ZIO.unit)
      job    <- awaitStatus(runner, id)
    } yield assertTrue(
      job.status == JobRunStatus.Completed,
      job.completedAt.isDefined
    )
  }

  private def testSubmitRecordsFailed = test("submit records Failed on effect failure") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id     <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, ZIO.fail(new Exception("boom")))
      job    <- awaitStatus(runner, id)
    } yield assertTrue(
      job.status == JobRunStatus.Failed,
      job.error.contains("boom")
    )
  }

  private def testSubmitRejectsDuplicate = test("submit rejects duplicate running job") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(ClubUrlName("dup-club")), None, RunTrigger.Cli, ZIO.never)
      result <- runner.submit(JobKind.Recruitment, Some(ClubUrlName("dup-club")), None, RunTrigger.Cli, ZIO.unit).either
    } yield assertTrue(
      result.isLeft,
      result.left.exists(_.isInstanceOf[JobConflictException])
    )
  }

  private def testSubmitAllowsDifferentClub = test("submit allows same kind with different club") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(ClubUrlName("club-a")), None, RunTrigger.Cli, ZIO.never)
      id2    <- runner.submit(JobKind.Recruitment, Some(ClubUrlName("club-b")), None, RunTrigger.Cli, ZIO.unit)
    } yield assertTrue(JobRunId.unwrap(id2).nonEmpty)
  }

  private def testSubmitAllowsDifferentKind = test("submit allows different kind with same club") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(ClubUrlName("club-c")), None, RunTrigger.Cli, ZIO.never)
      id2    <- runner.submit(JobKind.Membership, Some(ClubUrlName("club-c")), None, RunTrigger.Cli, ZIO.unit)
    } yield assertTrue(JobRunId.unwrap(id2).nonEmpty)
  }

  private def testStatusUnknown = test("status returns None for unknown id") {
    for {
      runner <- ZIO.service[JobRunner]
      result <- runner.status(JobRunId.wrap("nonexistent"))
    } yield assertTrue(result.isEmpty)
  }

  private def testRecentJobsOrdered = test("recentJobs returns ordered list") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id1    <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, ZIO.unit)
      _      <- awaitStatus(runner, id1)
      id2    <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, ZIO.unit)
      _      <- awaitStatus(runner, id2)
      // Wait briefly for any auto-follow-up MatchRef jobs to settle
      _      <- ZIO.sleep(500.millis)
      recent <- runner.recentJobs(50)
    } yield assertTrue(
      recent.size >= 2,
      recent.head.startedAt.isAfter(recent.last.startedAt) || recent.head.startedAt == recent.last.startedAt
    )
  }
}
