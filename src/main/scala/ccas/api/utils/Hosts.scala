package ccas.api.utils

import zio.http.Scheme.HTTPS
import zio.http.URL

object Hosts {
  val api: URL = URL.root.scheme(HTTPS).host("api.chess.com").addPath("pub")
  val website: URL = URL.root.scheme(HTTPS).host("www.chess.com")
}
