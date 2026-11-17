package ccas.cli

import java.io.IOException

import zio.{Console, IO, Task, Trace, UIO, Unsafe}

import ccas.utils.Abandonable

/** ZIO's own console, except that a read from stdin is [[Abandonable.keeping]]: Ctrl-C at any of the CLI's questions
  * exits at once (#513), and a question cancelled mid-read leaves its line for the next (#514). [[Main]] installs it
  * for every command; tests keep `TestConsole`.
  */
private[cli] final class StdinConsole private (reads: Task[String]) extends Console {
  private val live = Console.ConsoleLive

  def print(line: => Any)(implicit trace: Trace): IO[IOException, Unit]          = live.print(line)
  def printError(line: => Any)(implicit trace: Trace): IO[IOException, Unit]     = live.printError(line)
  def printLine(line: => Any)(implicit trace: Trace): IO[IOException, Unit]      = live.printLine(line)
  def printLineError(line: => Any)(implicit trace: Trace): IO[IOException, Unit] = live.printLineError(line)

  // Ended input still fails with `EOFException`, as the live console's does.
  def readLine(implicit trace: Trace): IO[IOException, String] =
    reads.refineToOrDie[IOException]
}

private[cli] object StdinConsole {

  /** The one console a command reads stdin through: one per process, so every question shares its kept read. */
  val make: UIO[Console] =
    Abandonable
      .keeping(Unsafe.unsafe(implicit u => Console.ConsoleLive.unsafe.readLine()))
      .map(new StdinConsole(_))
}
