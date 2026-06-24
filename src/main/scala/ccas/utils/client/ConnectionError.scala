package ccas.utils.client

import io.netty.handler.codec.PrematureChannelClosureException

import ccas.utils.json.JsonDecodingException

/** Single authority for what counts as a transient connection / network error from the Chess.com client's transport.
  *
  * Type-based: `UnknownHostException` (DNS) and every other `IOException` subtype (`ConnectException`,
  * `SocketException`, …) plus Netty's `PrematureChannelClosureException` count; HTTP-status, JSON-decode, and
  * already-wrapped errors do not. If a connection error ever surfaces as a non-`IOException` (none observed —
  * `api_fetch_failure.error_type` records `UnknownHostException`), add a case here.
  *
  * This predicate is internal to the client: it drives both `retryConnectionSchedule` (what to retry) and the final
  * "I retried and the network is still down" decision in `withRetries` (what to wrap as
  * [[NetworkUnavailableException]]). Application code should match the typed [[NetworkUnavailableException]] rather
  * than re-classify raw throwables.
  */
object ConnectionError {

  def isConnectionError(e: Throwable): Boolean = e match {
    case _: HttpStatusException              => false // HTTP status codes are an answer from the origin, not a network fault
    case _: JsonDecodingException            => false // we reached the origin; the body just didn't parse
    case _: NetworkUnavailableException      => false // already wrapped — don't reclassify / double-wrap
    case _: java.io.IOException              => true  // UnknownHostException (DNS), ConnectException, SocketException, …
    case _: PrematureChannelClosureException => true  // Netty mid-request channel close (not an IOException)
    case _                                   => false
  }
}
