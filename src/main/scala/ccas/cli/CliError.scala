package ccas.cli

/** A user-facing CLI failure carrying the process exit code to return.
  *
  * Exit-code convention: `1` for a runtime failure (job failed, submission error, bad response); `2` for a malformed
  * `--server` URL (parity with decline's usage-error exit code).
  */
final case class CliError(message: String, exitCode: Int) extends Exception(message) {
  // No stack trace — these are expected control-flow signals, not crashes.
  override def fillInStackTrace(): Throwable = this
}

/** The follow stream connected then dropped before the job finished (e.g. a >50s idle-timeout close on a long silent
  * phase — #150). Distinct from [[CliError]] so the caller, which knows the job id, can render a reattach hint rather
  * than a "server down" message — the detached job keeps running server-side.
  */
final case class StreamDropped(cause: String) extends Exception(cause) {
  override def fillInStackTrace(): Throwable = this
}

/** The server could not be reached, or the connection broke before it answered. Distinct from [[CliError]] so a follow
  * whose log has already ended can tell a server that stopped mid-job from one that answered with an error (#336).
  * Other callers handle it as they do any failure; uncaught, the dispatcher prints its message and exits 1.
  */
final case class ServerUnreachable(message: String) extends Exception(message) {
  override def fillInStackTrace(): Throwable = this
}
