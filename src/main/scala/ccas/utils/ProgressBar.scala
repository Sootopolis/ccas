package ccas.utils

import zio.UIO

/** Handle for a single progress bar managed by `CcasLogger`.
  *
  * Each call to `print` erases the entire display, updates this bar's output, and redraws
  * all bars. Log messages (`CcasLogger.info`, etc.) automatically route above the bars.
  *
  * `finish` removes this bar from the display. It is idempotent.
  *
  * Created via `CcasLogger.progressBar`.
  */
class ProgressBar private[utils] (id: Int, display: ProgressDisplay) {

  /** Render progress in-place. Formats `text` with a 20-character block bar and percentage
    * derived from `current` / `total`. Supports multiline text.
    */
  def print(current: Int, total: Int, text: String): UIO[Unit] = {
    val pct      = if (total == 0) 100 else (current * 100) / total
    val filled   = pct / 5
    val bar      = "\u2588" * filled + "\u2591" * (20 - filled)
    val full     = s"$text $bar $pct%"
    val lineCount = full.count(_ == '\n') + 1
    display.render(id, full, lineCount)
  }

  /** Remove this bar from the display. No-op if already removed. */
  val finish: UIO[Unit] = display.removeBar(id)
}
