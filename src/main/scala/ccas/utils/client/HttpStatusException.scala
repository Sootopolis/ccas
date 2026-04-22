package ccas.utils.client

import zio.ZIO
import zio.http.URL

class HttpStatusException(val statusCode: Int, val url: URL, val responseBody: String)
    extends Exception(s"HTTP $statusCode for: $url")

extension [R, A](effect: ZIO[R, Throwable, A])
  def onNotFound[R1 <: R, A1 >: A](recover: HttpStatusException => ZIO[R1, Throwable, A1]): ZIO[R1, Throwable, A1] =
    effect.catchSome { case e: HttpStatusException if e.statusCode == 404 => recover(e) }
