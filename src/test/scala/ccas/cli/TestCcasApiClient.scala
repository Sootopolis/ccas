package ccas.cli

import java.net.ServerSocket

import zio.*
import zio.http.Client
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

/** Verifies the transport-level behaviour of the live client: an unreachable server yields a friendly message. */
object TestCcasApiClient extends ZIOSpecDefault {

  /** Bind then immediately close to obtain a port that is guaranteed refused. */
  private val refusedPort: Task[Int] = ZIO.attempt {
    val socket = new ServerSocket(0)
    try { socket.getLocalPort }
    finally { socket.close() }
  }

  override def spec: Spec[Any, Throwable] = suite("TestCcasApiClient")(
    test("an unreachable server fails a request with ServerUnreachable, saying to start one") {
      for {
        port <- refusedPort
        result <- CcasApiClient
          .live(s"http://127.0.0.1:$port")
          .flatMap(_.getJson[String]("/api/jobs"))
          .provide(Client.default)
          .either
      } yield assertTrue(result match {
        case Left(e: ServerUnreachable) => e.message.contains("cannot reach") && e.message.contains("ccas server up")
        case _                          => false
      })
    },
    test("an unreachable server reaches the operator as 'error: cannot reach', with exit code 1") {
      for {
        port <- refusedPort
        code <- CcasApiClient
          .live(s"http://127.0.0.1:$port")
          .flatMap(_.getJson[String]("/api/jobs"))
          .provide(Client.default)
          .as(0)
          .catchAll(Dispatcher.reportFailure)
        err <- TestConsole.outputErr
      } yield assertTrue(code == 1, err.exists(_.startsWith("error: cannot reach")))
    },
    test("streamLines against an unreachable server fails with ServerUnreachable, not StreamDropped") {
      for {
        port <- refusedPort
        result <- CcasApiClient
          .live(s"http://127.0.0.1:$port")
          .flatMap(_.streamLines("/api/jobs/job-1/logs")(_ => ZIO.unit))
          .provide(Client.default)
          .either
      } yield assertTrue(result match {
        // Never reached the server, so it can't have dropped mid-stream.
        case Left(e: ServerUnreachable) => e.message.contains("cannot reach")
        case _                          => false
      })
    }
  )
}
