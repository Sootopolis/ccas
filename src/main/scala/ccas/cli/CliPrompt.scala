package ccas.cli

import java.io.EOFException

import zio.{Console, Task, ZIO}

/** The CLI's questions to the operator, read from stdin; a yes/no question goes through [[ccas.utils.YesNo]]. */
private[cli] object CliPrompt {

  /** Prints `label` and reads one line, trimmed. Input that has ended fails with `ended`, which names what was left
    * unanswered, in place of the bare "error: null" an `EOFException` would print.
    */
  def line(label: String, ended: String): Task[String] =
    (Console.print(label) *> readLine)
      .someOrFail(CliError(ended, 1))
      .map(_.trim)

  /** One line from stdin, or `None` once input has ended. */
  val readLine: Task[Option[String]] =
    Console.readLine.asSome.catchSome { case _: EOFException => ZIO.none }
}
