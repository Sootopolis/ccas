package ccas.cli

import java.lang.ProcessBuilder.Redirect

import zio.*

/** Draws the operator back to a terminal that is waiting on them (#533): a terminal bell and a desktop notification,
  * repeated until the wait ends, since one banner is easily missed.
  */
private[cli] object Attention {

  /** How often [[whileWaiting]] alerts again, and how many times, so a prompt left overnight stops after an hour. */
  val Every: Duration = 10.minutes
  val Repeats: Int    = 6

  /** Rings the terminal bell and shows `text` as a desktop notification. Both are best-effort: a terminal or desktop
    * that can't show them leaves the wait as it was.
    */
  def alert(title: String)(text: String): UIO[Unit] =
    Console.print("\u0007").ignore *> ZIO.foreachDiscard(notifier(Desktop.current, title, text))(run)

  /** Runs `wait`, alerting as it starts and again after each `every`, up to `repeats` times or until it ends. */
  def whileWaiting[A](alert: UIO[Unit], every: Duration, repeats: Int)(wait: UIO[A]): UIO[A] =
    alert *> ZIO.scoped((ZIO.sleep(every) *> alert).repeatN(repeats - 1).forkScoped *> wait)

  /** The command that shows a notification on `desktop`. The title and text are arguments, never script or options,
    * so whatever a club is called reaches the banner as written.
    */
  def notifier(desktop: Desktop, title: String, text: String): Option[List[String]] =
    desktop match {
      case Desktop.Mac =>
        val script = List("on run argv", "display notification (item 2 of argv) with title (item 1 of argv)", "end run")
        Some(("osascript" :: script.flatMap(line => List("-e", line))) ++ List(title, text))
      case Desktop.Wayland | Desktop.X11 => Some(List("notify-send", "--", title, text))
      case Desktop.Headless              => None
    }

  // Started, not awaited: a notifier stuck on a slow notification service must hold up neither the prompt nor the
  // answer, and the JVM reaps the process when it exits.
  private def run(command: List[String]): UIO[Unit] =
    ZIO.attemptBlocking {
      val builder = new ProcessBuilder(command*)
      builder.redirectOutput(Redirect.DISCARD)
      builder.redirectError(Redirect.DISCARD)
      builder.start().getOutputStream.close()
    }.ignore
}
