package ccas.utils

import java.util.concurrent.atomic.AtomicInteger

import zio.{Console, Ref, Scope, Semaphore, UIO, ZIO}

/** Manages progress bars rendered as a single line on stdout, overwritten in-place via `\r`.
  *
  * All stdout operations (`render`, `logAboveBars`, `removeBar`) are serialized via a
  * `Semaphore(1)` to ensure atomic erase-print-redraw sequences.
  *
  * When `enabled` is false all methods are no-ops — suitable for server/non-interactive mode.
  *
  * Created via `CcasLogger.progressBar` or `CcasLogger.progressDisplay`.
  */
class ProgressDisplay private[utils] (
  state: Ref[List[ProgressDisplay.BarState]],
  mutex: Semaphore,
  private val enabled: Boolean
) {

  private val idGen = new AtomicInteger(0)

  // ---------------------------------------------------------------------------
  // Bar lifecycle
  // ---------------------------------------------------------------------------

  /** Create a new progress bar appended to the bottom of the display. */
  def addBar: UIO[ProgressBar] =
    for {
      id <- ZIO.succeed(idGen.getAndIncrement())
      _  <- state.update(_ :+ ProgressDisplay.BarState(id, 0, ""))
    } yield new ProgressBar(id, this)

  /** Create a scoped progress bar — automatically removed when the scope closes. */
  def addBarScoped: ZIO[Scope, Nothing, ProgressBar] =
    ZIO.acquireRelease(addBar)(_.finish)

  // ---------------------------------------------------------------------------
  // Rendering (called by ProgressBar)
  // ---------------------------------------------------------------------------

  /** Update one bar's output and re-render the whole display. */
  private[utils] def render(barId: Int, output: String, lineCount: Int): UIO[Unit] =
    ZIO.whenDiscard(enabled)(mutex.withPermit {
      for {
        bars <- state.get
        updated = bars.map { b =>
          if (b.id == barId) ProgressDisplay.BarState(barId, lineCount, output)
          else b
        }
        _ <- state.set(updated)
        _ <- drawAll(updated)
      } yield ()
    })

  /** Remove a bar from the display. */
  private[utils] def removeBar(barId: Int): UIO[Unit] =
    ZIO.whenDiscard(enabled)(mutex.withPermit {
      for {
        bars <- state.get
        _ <- clearLine(bars)
        updated = bars.filterNot(_.id == barId)
        _ <- state.set(updated)
        _ <- drawAll(updated)
      } yield ()
    })

  /** Finish all bars — erase without redraw. Called from scope finalizer. */
  private[utils] def finishAll: UIO[Unit] =
    ZIO.whenDiscard(enabled)(mutex.withPermit {
      for {
        bars <- state.get
        _ <- clearLine(bars)
        _ <- state.set(Nil)
      } yield ()
    })

  // ---------------------------------------------------------------------------
  // Logging (called by CcasLogger)
  // ---------------------------------------------------------------------------

  /** Print a log message above the progress bars without disrupting them. */
  private[utils] def logAboveBars(msg: String): UIO[Unit] =
    if (!enabled) {
      Console.printLine(msg).ignore
    } else {
      mutex.withPermit {
        for {
          bars <- state.get
          hasRendered = bars.exists(_.lineCount > 0)
          _ <- ZIO.whenDiscard(hasRendered)(clearLine(bars))
          _ <- Console.printLine(msg).ignore
          _ <- ZIO.whenDiscard(hasRendered)(drawAll(bars))
        } yield ()
      }
    }

  // ---------------------------------------------------------------------------
  // Internal rendering — single-line, \r-based (no cursor-up needed)
  // ---------------------------------------------------------------------------

  /** Clear the current bar line. */
  private def clearLine(bars: List[ProgressDisplay.BarState]): UIO[Unit] = {
    val hasOutput = bars.exists(_.lineCount > 0)
    ZIO.whenDiscard(hasOutput)(Console.print("\r\u001b[K").ignore)
  }

  /** Render all active bars as a single \r-overwritten line. */
  private def drawAll(bars: List[ProgressDisplay.BarState]): UIO[Unit] = {
    val parts = bars.filter(_.lineCount > 0).map(_.lastOutput.trim)
    ZIO.whenDiscard(parts.nonEmpty)(Console.print("\r" + parts.mkString("  ") + "\u001b[K").ignore)
  }
}

object ProgressDisplay {
  private[utils] case class BarState(id: Int, lineCount: Int, lastOutput: String)

  private[utils] def make(enabled: Boolean): UIO[ProgressDisplay] =
    for {
      ref <- Ref.make(List.empty[BarState])
      sem <- Semaphore.make(1)
    } yield new ProgressDisplay(ref, sem, enabled)
}
