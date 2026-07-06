package ccas.server.jobs

import zio.stream.ZStream
import zio.{Duration, Schedule, durationInt}

/** Wire protocol for the `GET /api/jobs/{id}/logs` chunked follow stream.
  *
  * The `/logs` response only carries bytes when a new log line lands, so any job phase that goes silent for longer than
  * the follower's idle timeout (zio-http's `Client.default` closes an idle connection after 50s) drops the follower —
  * even though the detached job keeps running server-side (issue #150). [[withKeepAlive]] interleaves a periodic
  * zero-information keepalive line so bytes always flow during silence; the follower ([[ccas.cli.CcasApiClient.streamLines]])
  * filters [[KeepAliveLine]] back out.
  */
object JobLogStream {

  /** The keepalive marker interleaved during silent stretches. A NUL (U+0000) can never collide with a real
    * `ProgressDisplay`-formatted, ANSI/source-tag-stripped log line, and renders invisibly to a raw `curl` follower.
    * Built from `0.toChar` rather than a source escape/control byte so the source stays plain ASCII.
    */
  val KeepAliveLine: String = 0.toChar.toString

  /** Spacing between keepalive ticks — comfortably under the 50s idle timeout so at least two ticks land per window. */
  val KeepAliveInterval: Duration = 20.seconds

  /** Interleave keepalive ticks into `lines`, halting the moment `lines` completes so the "stream close = job done"
    * contract still holds (`mergeHaltLeft` terminates the merged stream when the left stream ends, interrupting the
    * otherwise-infinite tick). `Schedule.fixed` gives a steady wall-clock cadence (its first tick lands after
    * [[KeepAliveInterval]], not immediately) independent of when lines happen to flow.
    */
  def withKeepAlive(lines: ZStream[Any, Throwable, String]): ZStream[Any, Throwable, String] =
    lines.mergeHaltLeft(ZStream.fromSchedule(Schedule.fixed(KeepAliveInterval)).as(KeepAliveLine))
}
