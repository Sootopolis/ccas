package ccas.utils.client

import zio.{RIO, ZIO}

import ccas.utils.errors.safeMessage

/** Typed contract error surfaced by `ChessComClient` when a connection / network error survives the client's
  * connection-retry schedule — i.e. the network has been unreachable across all retry attempts (~7s by default), not
  * a one-off blip. The underlying transport exception is preserved as the cause (and recorded verbatim in
  * `api_fetch_failure` before the wrap), so telemetry keeps the real type while callers get a single type to match on.
  *
  * Apps match this type to react to a systemic outage (abort, flush partial-but-valid data, log, …); only the client
  * decides when to throw it (via [[ConnectionError.isConnectionError]] after retries exhaust).
  */
final class NetworkUnavailableException(cause: Throwable)
    extends Exception(s"Network unavailable: ${cause.safeMessage}", cause)

object NetworkUnavailableException {

  /** For use in a `foldZIO` / `catchAll` error arm: re-raise a systemic [[NetworkUnavailableException]] so the run
    * aborts; otherwise run `recover`. Per-resource swallow-sites (a failed-URL record, a `NotFound`, …) call this so a
    * network outage that reaches a deeper fetch isn't masked as ordinary per-resource state. Keyed on the caught error
    * — not the whole effect — so it never intercepts failures from the success path (DB writes, verification, …).
    * Shared across apps; only the client throws the type, every consumer reuses this guard.
    */
  def recoverUnless[R, A](error: Throwable)(recover: => RIO[R, A]): RIO[R, A] =
    error match {
      case e: NetworkUnavailableException => ZIO.fail(e)
      case _                              => recover
    }
}
