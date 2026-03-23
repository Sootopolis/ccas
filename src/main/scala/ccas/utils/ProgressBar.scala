package ccas.utils

import zio.{Console, Ref, Scope, UIO, ZIO}

/** In-place console progress bar using ANSI escape codes.
  *
  * Each call to `print` erases the previous output (even across multiple lines) and rewrites
  * with the new text plus a visual bar and percentage. Use `finish` (or `scoped` for automatic
  * clean-up) to emit a final newline and reset state.
  *
  * Messages logged via `logInfo`, `logWarning`, etc. are printed above the bar without
  * disrupting it — the bar is erased, the message is written on its own line, and the bar
  * is redrawn.
  *
  * `finish` is idempotent — safe to call explicitly before the scope closes.
  */
class ProgressBar private (state: Ref[ProgressBar.State]) {

  /** Render progress in-place. Erases any previously printed lines, then writes `text` followed
    * by a 20-character block bar and percentage derived from `current` / `total`.
    * Supports multiline text — tracks line count for correct erasure on next call.
    */
  def print(current: Int, total: Int, text: String): UIO[Unit] = {
    val pct      = if (total == 0) 100 else (current * 100) / total
    val filled   = pct / 5
    val bar      = "\u2588" * filled + "\u2591" * (20 - filled)
    val full     = s"$text $bar $pct%"
    val parts    = full.split("\n", -1)
    val rendered = parts.mkString("\n")
    val lines    = parts.length
    for {
      prev  <- state.getAndSet(ProgressBar.State(lines, rendered))
      erase  = "\u001b[A\u001b[2K" * prev.lineCount
      output = erase + "\r" + rendered
      _     <- Console.print(output).ignore
    } yield ()
  }

  /** Print a message above the progress bar without disrupting it.
    * The bar is erased, the message is written, then the bar is redrawn.
    */
  def logDebug(msg: String): UIO[Unit]   = log("DEBUG", msg)
  def logInfo(msg: String): UIO[Unit]    = log("INFO", msg)
  def logWarning(msg: String): UIO[Unit] = log("WARN", msg)
  def logError(msg: String): UIO[Unit]   = log("ERROR", msg)

  private def log(level: String, msg: String): UIO[Unit] =
    for {
      s     <- state.get
      erase  = "\u001b[A\u001b[2K" * s.lineCount
      redraw = s.lastOutput
      _     <- Console.print(s"$erase\r[$level] $msg\n$redraw").ignore
    } yield ()

  /** Emit a newline to move past the bar. No-op if nothing was printed. */
  val finish: UIO[Unit] = state.getAndSet(ProgressBar.State(0, "")).flatMap { prev =>
    ZIO.whenDiscard(prev.lineCount > 0)(Console.printLine("").ignore)
  }
}

object ProgressBar {

  private[utils] case class State(lineCount: Int, lastOutput: String)

  /** Create a progress bar with manual lifecycle — caller must invoke `finish`. */
  def make: UIO[ProgressBar] = Ref.make(State(0, "")).map(new ProgressBar(_))

  /** Create a progress bar that calls `finish` automatically when the scope closes
    * (on completion, failure, or interruption).
    */
  def scoped: ZIO[Scope, Nothing, ProgressBar] = ZIO.acquireRelease(make)(_.finish)
}
