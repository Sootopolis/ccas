package ccas.utils.client

import zio.http.URL
import zio.test.*

object TestHttpStatusException extends ZIOSpecDefault {

  private val url = URL.decode("https://api.chess.com/pub/match/1929957").toOption.get

  private val permanentBody = """{"code": 0, "message": "Match \"1929957\" not found."}"""
  private val transientBody = """{"code": 3024, "message": "An internal error has occurred. Please contact admin."}"""

  override def spec: Spec[TestEnvironment, Any] = suite("HttpStatusException.isPermanentNotFound")(
    test("404 with permanent body is classified permanent") {
      val e = HttpStatusException(404, url, permanentBody)
      assertTrue(e.isPermanentNotFound)
    },
    test("404 with transient body is not classified permanent") {
      val e = HttpStatusException(404, url, transientBody)
      assertTrue(!e.isPermanentNotFound)
    },
    test("non-404 with permanent body shape is not classified permanent") {
      val e500 = HttpStatusException(500, url, permanentBody)
      val e200 = HttpStatusException(200, url, permanentBody)
      assertTrue(!e500.isPermanentNotFound, !e200.isPermanentNotFound)
    },
    test("404 with empty body is not classified permanent") {
      val e = HttpStatusException(404, url, "")
      assertTrue(!e.isPermanentNotFound)
    }
  )
}
