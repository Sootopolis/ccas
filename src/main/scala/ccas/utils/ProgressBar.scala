package ccas.utils

import zio.{Console, Ref, Scope, UIO, ZIO}

/** In-place console progress bar using ANSI escape codes.
  *
  * Each call to `print` erases the previous output (even across multiple lines) and rewrites
  * with the new text plus a visual bar and percentage. Use `finish` (or `scoped` for automatic
  * clean-up) to emit a final newline and reset state.
  *
  * `finish` is idempotent — safe to call explicitly before the scope closes.
  */
class ProgressBar private (lastLineCount: Ref[Int]) {

  /** Render progress in-place. Erases any previously printed lines, then writes `text` followed
    * by a 20-character block bar and percentage derived from `current` / `total`.
    * Supports multiline text — tracks line count for correct erasure on next call.
    */
  def print(current: Int, total: Int, text: String): UIO[Unit] = {
    val pct    = if (total == 0) 100 else (current * 100) / total
    val filled = pct / 5
    val bar    = "\u2588" * filled + "\u2591" * (20 - filled)
    val full   = s"$text $bar $pct%"
    val lines  = full.split("\n", -1)
    for {
      prev  <- lastLineCount.getAndSet(lines.length)
      erase  = "\u001b[A\u001b[2K" * prev
      output = erase + "\r" + lines.mkString("\n")
      _     <- Console.print(output).ignore
    } yield ()
  }

  /** Emit a newline to move past the bar. No-op if nothing was printed. */
  val finish: UIO[Unit] = lastLineCount.getAndSet(0).flatMap { prev =>
    ZIO.whenDiscard(prev > 0)(Console.printLine("").ignore)
  }
}

object ProgressBar {

  /** Create a progress bar with manual lifecycle — caller must invoke `finish`. */
  def make: UIO[ProgressBar] = Ref.make(0).map(new ProgressBar(_))

  /** Create a progress bar that calls `finish` automatically when the scope closes
    * (on completion, failure, or interruption).
    */
  def scoped: ZIO[Scope, Nothing, ProgressBar] = ZIO.acquireRelease(make)(_.finish)
}
