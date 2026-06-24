package ccas.utils.client

import zio.{Exit, RIO, ZIO}
import zio.http.URL
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Unit contract for the `swallowRecoveryErrors` extension (issue #119): a systemic outage re-raises so the run
  * aborts; expected HTTP errors and any other unexpected error resolve to `None`; success passes through unchanged.
  */
object TestRecoveryErrors extends ZIOSpecDefault {

  private val url: URL = URL.decode("http://test.example.com/x").toOption.get

  override def spec: Spec[Any, Throwable] = suite("TestRecoveryErrors")(
    test("NetworkUnavailableException re-raises (aborts)") {
      val eff: RIO[Any, Option[Int]] =
        ZIO.fail(new NetworkUnavailableException(new java.net.UnknownHostException("down")))
      eff.swallowRecoveryErrors("ctx").exit.map { exit =>
        assertTrue(exit match {
          case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[NetworkUnavailableException])
          case _                   => false
        })
      }
    },
    test("HttpStatusException → None (expected, silent)") {
      val eff: RIO[Any, Option[Int]] = ZIO.fail(HttpStatusException(500, url, "boom"))
      eff.swallowRecoveryErrors("ctx").map(r => assertTrue(r.isEmpty))
    },
    test("ReportedNotFound (404) → None") {
      val eff: RIO[Any, Option[Int]] = ZIO.fail(ReportedNotFound(url, "not found.\""))
      eff.swallowRecoveryErrors("ctx").map(r => assertTrue(r.isEmpty))
    },
    test("other Throwable (DB / decode) → None (debug-logged)") {
      val eff: RIO[Any, Option[Int]] = ZIO.fail(new RuntimeException("decode boom"))
      eff.swallowRecoveryErrors("ctx").map(r => assertTrue(r.isEmpty))
    },
    test("success passes through") {
      val eff: RIO[Any, Option[Int]] = ZIO.some(5)
      eff.swallowRecoveryErrors("ctx").map(r => assertTrue(r.contains(5)))
    }
  )
}
