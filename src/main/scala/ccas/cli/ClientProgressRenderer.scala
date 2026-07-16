package ccas.cli

import zio.{Ref, UIO, ZIO}

import ccas.utils.{BarSnapshot, ProgressBar, ProgressDisplay, ProgressSnapshot}

/** Renders the bar frames streamed from `GET /api/jobs/{id}/progress` onto a local [[ProgressDisplay]] while the CLI
  * follows a job. Each frame is a full latest-wins snapshot, so [[render]] reconciles it against the bars currently
  * shown: update a bar we already track (skipping the redraw when its raw fields are unchanged), add one that's new,
  * finish one that dropped out of the frame. The server sends raw `current` / `total` / `text`; [[ProgressBar.print]]
  * re-renders the block bar + percentage at this terminal's width (the server can't, it doesn't know the width).
  *
  * `bars` maps a server-assigned bar id to the local [[ProgressBar]] standing in for it plus the last snapshot rendered
  * for it — the two id spaces are independent, so this indirection is what lets successive frames address the same bar,
  * and the stored snapshot lets an unchanged bar skip a redundant full-display redraw.
  */
final class ClientProgressRenderer private[cli] (
  display: ProgressDisplay,
  bars: Ref[Map[Int, ClientProgressRenderer.Tracked]]
) {
  import ClientProgressRenderer.Tracked

  /** Reconcile one snapshot frame onto the display. */
  def render(frame: ProgressSnapshot): UIO[Unit] =
    bars.get.flatMap { current =>
      val incomingIds = frame.bars.map(_.id).toSet
      val stale       = current.keySet -- incomingIds
      ZIO.foreachDiscard(frame.bars)(upsert(current, _)) *>
        ZIO.foreachDiscard(stale)(drop(current, _))
    }

  // Update the local bar for this snapshot's id, or create one if it's new. A frame carries every live bar even when
  // only one changed, so skip the print (and its whole-display redraw) when the raw fields are identical to last time.
  private def upsert(current: Map[Int, Tracked], snap: BarSnapshot): UIO[Unit] =
    current.get(snap.id) match {
      case Some(t) if t.last == snap => ZIO.unit
      case Some(t) =>
        t.bar.print(snap.current, snap.total, snap.text) *> bars.update(_.updated(snap.id, Tracked(t.bar, snap)))
      case None =>
        for {
          bar <- display.addBar
          _   <- bars.update(_ + (snap.id -> Tracked(bar, snap)))
          _   <- bar.print(snap.current, snap.total, snap.text)
        } yield ()
    }

  // A bar absent from the latest frame is finished (erased) and forgotten.
  private def drop(current: Map[Int, Tracked], id: Int): UIO[Unit] =
    ZIO.foreachDiscard(current.get(id))(_.bar.finish) *> bars.update(_ - id)

  /** Finish every remaining bar and forget them — called when the follow ends so the terminal is left clean (the
    * `/progress` stream may close before a final empty frame arrives, leaving bars drawn). Idempotent.
    */
  def clear: UIO[Unit] =
    bars.getAndSet(Map.empty).flatMap(m => ZIO.foreachDiscard(m.values)(_.bar.finish))

  /** Print a job-log line above the bars, interleaved cleanly via the display's render lock. */
  def logLine(line: String): UIO[Unit] =
    display.logLineAboveBars(line)
}

object ClientProgressRenderer {

  /** A tracked bar: the local [[ProgressBar]] plus the last [[BarSnapshot]] rendered onto it (for change detection). */
  final case class Tracked(bar: ProgressBar, last: BarSnapshot)

  /** Build a renderer over a fresh [[ProgressDisplay]] drawing to `System.out`. Only ever built when bars are wanted
    * (interactive TTY), so the display is always enabled.
    */
  def make: UIO[ClientProgressRenderer] =
    Ref.make(Map.empty[Int, Tracked]).map(new ClientProgressRenderer(ProgressDisplay.make(enabled = true), _))
}
