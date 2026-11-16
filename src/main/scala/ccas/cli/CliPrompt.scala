package ccas.cli

import java.io.EOFException

import zio.{Console, Task, ZIO}

/** The CLI's questions to the operator, read from stdin; a yes/no question is [[ccas.utils.YesNo.ask]]. */
private[cli] object CliPrompt {

  /** Prints `label` and reads one line, trimmed. Input that has ended fails with `ended`, which names what was left
    * unanswered, in place of the bare "error: null" an `EOFException` would print.
    */
  def line(label: String, ended: String): Task[String] =
    (Console.print(label) *> Console.readLine)
      .map(_.trim)
      .catchSome { case _: EOFException => ZIO.fail(CliError(ended, 1)) }
}
