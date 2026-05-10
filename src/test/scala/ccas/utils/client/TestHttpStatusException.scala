package ccas.utils.client

import zio.http.URL
import zio.test.*

object TestHttpStatusException extends ZIOSpecDefault {

  private val url = URL.decode("https://api.chess.com/pub/match/1929957").toOption.get

  private val reportedBody  = """{"code": 0, "message": "Match \"1929957\" not found."}"""
  private val transientBody = """{"code": 3024, "message": "An internal error has occurred. Please contact admin."}"""

  override def spec: Spec[TestEnvironment, Any] = suite("HttpStatusException.classify")(
    test("404 with reported body produces ReportedNotFound") {
      assertTrue(HttpStatusException.classify(404, url, reportedBody).isInstanceOf[ReportedNotFound])
    },
    test("404 with transient body produces base HttpStatusException, not ReportedNotFound") {
      val e = HttpStatusException.classify(404, url, transientBody)
      assertTrue(!e.isInstanceOf[ReportedNotFound], e.statusCode == 404)
    },
    test("non-404 with reported body shape stays as base HttpStatusException") {
      val e500 = HttpStatusException.classify(500, url, reportedBody)
      val e200 = HttpStatusException.classify(200, url, reportedBody)
      assertTrue(!e500.isInstanceOf[ReportedNotFound], !e200.isInstanceOf[ReportedNotFound])
    },
    test("404 with empty body stays as base HttpStatusException") {
      val e = HttpStatusException.classify(404, url, "")
      assertTrue(!e.isInstanceOf[ReportedNotFound], e.statusCode == 404)
    }
  )
}
