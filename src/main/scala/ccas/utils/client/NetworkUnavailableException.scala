package ccas.utils.client

import zio.{RIO, UIO, ZIO}

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

  /** `catchSome` arm for a top-level app `run`: on a systemic outage, log one clean line and exit non-zero. Shared by
    * every `ZIOApp` that aborts on an outage (RefApp, Membership, History, Recruitment) so the message template and
    * exit handling live in one place. `detail` is the per-app consequence (e.g. "no skips recorded", "run left
    * incomplete"); `onAbort` is the app's own `exit(ExitCode.failure)` (kept at the callsite since it resolves against
    * the `ZIOApp` instance). `ZIO.fail` would double-report through `ZIOAppDefault`'s default handler, hence `exit`.
    */
  def abortRun(app: String, detail: String)(onAbort: UIO[Unit]): PartialFunction[Throwable, UIO[Unit]] = {
    case e: NetworkUnavailableException =>
      ZIO.logError(s"$app aborted — network unavailable; $detail: ${e.safeMessage}") *> onAbort
  }
}

/** Recovery-internal swallow for tiered rename / slug resolution: a tier's own failure must never replace the
  * caller's original 404, but a systemic outage must still abort the run rather than record a bogus skip (#119).
  * The tier design is `docs/adr/0010-rename-recovery-for-usernames-and-club-slugs.md`.
  *
  *   - [[NetworkUnavailableException]] — re-raise (systemic outage, abort the run).
  *   - [[HttpStatusException]] — `None`, silently (cancelled match, missing tournament, intermittent 5xx).
  *   - anything else (DB / decode) — debug-log for triage, then `None`.
  */
extension [R, A](self: RIO[R, Option[A]])
  def swallowRecoveryErrors(context: String): RIO[R, Option[A]] =
    self.catchAll {
      case e: NetworkUnavailableException => ZIO.fail(e)
      case _: HttpStatusException         => ZIO.none
      case e => ZIO.logDebug(s"  recovery internal error ($context): ${e.getMessage}").as(None)
    }
