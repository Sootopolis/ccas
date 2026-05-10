package ccas.utils.client

import zio.ZIO
import zio.http.URL

class HttpStatusException(val statusCode: Int, val url: URL, val responseBody: String)
    extends Exception(s"HTTP $statusCode for: $url")

/** A 404 whose body is the canonical Chess.com `"X \"id\" not found."` shape (across `/match`, `/player`, `/club`,
  * `/tournament`). Distinguishes server-reported "this name resolves to nothing" from transient backend failures
  * that also surface as 404 with an `"An internal error has occurred"` body (codes 0 / 3024 / 403). The body
  * substring match is sufficient until #3 lands a structured body classifier.
  *
  * Note: "reported" rather than "permanent" — per #27, a `not found` on Chess.com is timeline-unstable. Slugs and
  * usernames can flip 404→200 if the original holder renames back, or if a different account registers the freed
  * handle. Callers must treat this as "missing right now," not "missing forever."
  */
class ReportedNotFound(u: URL, b: String) extends HttpStatusException(404, u, b)

object HttpStatusException {

  /** Construct the right subclass for a failed HTTP response. Used at the single throw site in `ChessComClient`
    * and by tests that fabricate exceptions directly.
    *
    * The match anchors on the JSON-string closing quote (`not found."`), not the bare phrase, so a body that
    * happens to mention "not found." mid-sentence won't classify — only canonical Chess.com bodies of the form
    * `{"code":0,"message":"X \"id\" not found."}` reach `ReportedNotFound`. Case-sensitive on purpose:
    * Chess.com's production wire is lowercase per the #3 survey, so loosening would mask test-fixture drift
    * rather than catch a real signal. Test fixtures should mirror the production wire shape verbatim.
    *
    * Future work tracked in #3: replace this with a JSON parse of the body into `(code, message)` and key off
    * the structured fields. Bundles cleanly with the planned `api_fetch_failure.body_code` /
    * `body_message_kind` columns, hence deferred from the current change.
    *
    * Telemetry note: `api_fetch_failure.error_type` is populated from `e.getClass.getSimpleName` at the throw
    * site, so 404s previously stamped as `HttpStatusException` will now stamp as `ReportedNotFound` for the ~5%
    * with the canonical body. Aggregations that span the rollout boundary should treat both labels as the same
    * shape.
    */
  def classify(statusCode: Int, url: URL, body: String): HttpStatusException =
    if (statusCode == 404 && body.contains("not found.\"")) ReportedNotFound(url, body)
    else HttpStatusException(statusCode, url, body)
}

extension [R, A](effect: ZIO[R, Throwable, A])
  def onNotFound[R1 <: R, A1 >: A](recover: HttpStatusException => ZIO[R1, Throwable, A1]): ZIO[R1, Throwable, A1] =
    effect.catchSome { case e: HttpStatusException if e.statusCode == 404 => recover(e) }
