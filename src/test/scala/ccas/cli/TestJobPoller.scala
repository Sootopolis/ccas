package ccas.cli

import zio.*
import zio.json.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.server.routes.JobRoutes.{ClubJobResult, JobResult, JobStatusResponse}

/** Exercises the poll loop and submit-result handling against a scripted stub client — no socket, no DB. */
object TestJobPoller extends ZIOSpecDefault {

  /** Stub that replays a fixed list of `JobStatusResponse` JSON bodies for `getJson`. */
  private final class StubApi(script: Ref[List[String]]) extends CcasApiClient {
    override def getJson[Resp: JsonDecoder](path: String): Task[Resp] =
      script.modify {
        case head :: tail => (head, tail)
        case Nil          => ("", Nil)
      }.flatMap(s => ZIO.fromEither(s.fromJson[Resp]).mapError(m => CliError(s"stub decode failed: $m", 1)))

    override def postJson[Req: JsonEncoder, Resp: JsonDecoder](path: String, body: Req): Task[Resp] =
      ZIO.die(new UnsupportedOperationException("postJson"))
    override def postEmpty[Resp: JsonDecoder](path: String): Task[Resp] =
      ZIO.die(new UnsupportedOperationException("postEmpty"))
    override def postUnit[Req: JsonEncoder](path: String, body: Req): Task[Unit] = ZIO.unit
    override def delete(path: String): Task[Unit]                                = ZIO.unit
  }

  private def statusJson(status: String, error: Option[String]): String =
    JobStatusResponse("job-1", "Membership", status, None, "2026-01-01T00:00:00Z", None, error, "Cli").toJson

  private def pollerWith(statuses: String*): UIO[JobPoller] =
    Ref.make(statuses.toList).map(ref => JobPoller(new StubApi(ref), 1.milli, 1.minute))

  override def spec: Spec[Any, Throwable] = suite("TestJobPoller")(
    test("pollOne returns 0 when the job completes") {
      for {
        poller <- pollerWith(statusJson("Completed", None))
        code   <- poller.pollOne("job-1")
      } yield assertTrue(code == 0)
    },
    test("pollOne returns 1 when the job fails") {
      for {
        poller <- pollerWith(statusJson("Failed", Some("boom")))
        code   <- poller.pollOne("job-1")
      } yield assertTrue(code == 1)
    },
    test("pollOne loops through Running then Completed") {
      for {
        poller <- pollerWith(statusJson("Running", None), statusJson("Completed", None))
        code   <- poller.pollOne("job-1")
      } yield assertTrue(code == 0)
    },
    test("pollOne exits 1 on an unexpected status rather than looping") {
      for {
        poller <- pollerWith(statusJson("Bananas", None))
        code   <- poller.pollOne("job-1")
      } yield assertTrue(code == 1)
    },
    test("pollOne times out when the job never terminates") {
      for {
        ref <- Ref.make(List.fill(10000)(statusJson("Running", None)))
        poller = JobPoller(new StubApi(ref), 1.milli, 30.millis)
        code <- poller.pollOne("job-1")
      } yield assertTrue(code == 1)
    },
    test("handleSingle short-circuits on a submission error") {
      for {
        poller <- pollerWith()
        code   <- poller.handleSingle("recruit", JobResult(None, Some("Club not found")))
      } yield assertTrue(code == 1)
    },
    test("handleBatch fails overall if any job fails to submit") {
      for {
        poller <- pollerWith(statusJson("Completed", None))
        code <- poller.handleBatch(
          List(
            ClubJobResult("a", Some("job-1"), None),
            ClubJobResult("b", None, Some("Club not found"))
          )
        )
      } yield assertTrue(code == 1)
    }
  ) @@ TestAspect.withLiveClock
}
