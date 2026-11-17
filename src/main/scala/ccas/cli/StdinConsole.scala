package ccas.cli

import java.io.IOException

import zio.{Console, IO, Trace, Unsafe}

import ccas.utils.Abandonable

/** ZIO's own console, except that a read from stdin is [[Abandonable]], so Ctrl-C at any of the CLI's questions exits
  * at once (#513). [[Main]] installs it for every command; tests keep `TestConsole`.
  */
private[cli] object StdinConsole extends Console {
  private val live = Console.ConsoleLive

  def print(line: => Any)(implicit trace: Trace): IO[IOException, Unit]          = live.print(line)
  def printError(line: => Any)(implicit trace: Trace): IO[IOException, Unit]     = live.printError(line)
  def printLine(line: => Any)(implicit trace: Trace): IO[IOException, Unit]      = live.printLine(line)
  def printLineError(line: => Any)(implicit trace: Trace): IO[IOException, Unit] = live.printLineError(line)

  // Ended input still fails with `EOFException`, as the live console's does.
  def readLine(implicit trace: Trace): IO[IOException, String] =
    Abandonable.run(Unsafe.unsafe(implicit u => live.unsafe.readLine())).refineToOrDie[IOException]
}
