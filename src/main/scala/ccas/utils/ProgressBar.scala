package ccas.utils

import zio.UIO

/** Handle for a single progress bar managed by `ProgressDisplay`.
  *
  * Each call to `print` erases the entire display, updates this bar's output, and redraws all bars. Log messages
  * routed through ZIO's logging (`ZIO.logInfo`, etc.) automatically interleave above the bars via the
  * `asZLogger` accessor that `ProgressDisplay.live` installs.
  *
  * `finish` removes this bar from the display. It is idempotent.
  *
  * Created via `ProgressDisplay.progressBar` or `ProgressDisplay.addBarScoped`.
  */
class ProgressBar private[utils] (id: Int, display: ProgressDisplay) {

  /** Render progress in-place. Formats `text` with a 20-character block bar and percentage derived from `current` /
    * `total`. Supports multiline text.
    */
  def print(current: Int, total: Int, text: String): UIO[Unit] = {
    val pct       = if (total == 0) 100.0 else (current * 100.0) / total
    val filled    = (pct / 5).toInt.min(20)
    val bar       = "\u2588" * filled + "\u2591" * (20 - filled)
    val full      = f"$text $bar $pct%.1f%%"
    val lineCount = full.count(_ == '\n') + 1
    // Pass both the rendered line (for the local terminal draw) and the raw fields (mirrored to this bar's channel as
    // an unrendered `BarSnapshot`, so a following CLI re-renders the block bar at its own terminal width).
    display.render(id, full, lineCount, current, total, text)
  }

  /** Remove this bar from the display. No-op if already removed. */
  val finish: UIO[Unit] = display.removeBar(id)
}
