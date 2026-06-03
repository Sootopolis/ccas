package ccas.cli

import zio.*
import zio.http.Client
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Verifies the transport-level behaviour of the live client: an unreachable server yields the friendly CliError. */
object TestCcasApiClient extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestCcasApiClient")(
    test("unreachable server yields a friendly CliError with exit code 1") {
      for {
        // Bind then immediately close to obtain a port that is guaranteed refused.
        port <- ZIO.attempt {
          val socket = new java.net.ServerSocket(0)
          try { socket.getLocalPort }
          finally { socket.close() }
        }
        result <- CcasApiClient
          .live(s"http://127.0.0.1:$port")
          .flatMap(_.getJson[String]("/api/jobs"))
          .provide(Client.default)
          .either
      } yield assertTrue(result match {
        case Left(e: CliError) => e.message.contains("cannot reach") && e.exitCode == 1
        case _                 => false
      })
    }
  )
}
