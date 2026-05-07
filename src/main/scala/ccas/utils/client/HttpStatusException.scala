package ccas.utils.client

import zio.ZIO
import zio.http.URL

class HttpStatusException(val statusCode: Int, val url: URL, val responseBody: String)
    extends Exception(s"HTTP $statusCode for: $url")

extension [R, A](effect: ZIO[R, Throwable, A])
  def onNotFound[R1 <: R, A1 >: A](recover: HttpStatusException => ZIO[R1, Throwable, A1]): ZIO[R1, Throwable, A1] =
    effect.catchSome { case e: HttpStatusException if e.statusCode == 404 => recover(e) }

extension (e: HttpStatusException)
  // Chess.com's permanent 404 body shape, observed across /match, /player, /club, /tournament endpoints:
  // {"code": 0, "message": "X \"123\" not found."}. Transient backend 404s use "An internal error has
  // occurred" with codes 0/3024/403 and never contain "not found.". Substring match is sufficient until #3
  // lands a structured body classifier.
  def isPermanentNotFound: Boolean =
    e.statusCode == 404 && e.responseBody.contains("not found.")
