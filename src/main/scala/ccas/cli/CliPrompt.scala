package ccas.cli

import java.io.EOFException

import zio.{Console, Task, ZIO}

import ccas.utils.Question

/** The CLI's questions to the operator, asked on the console through [[ccas.utils.Question]]; a yes/no question goes
  * through [[ccas.utils.YesNo]].
  */
private[cli] object CliPrompt {

  /** Prints `label` and reads lines until `answer` takes one, trimmed, showing the problem with each it refuses. Input
    * that has ended fails with `ended`, which names what was left unanswered, in place of the bare "error: null" an
    * `EOFException` would print.
    */
  def ask[A](label: String, ended: String)(answer: String => Task[Either[String, A]]): Task[A] =
    Question.ask(label, readLine, Console.print(_), ZIO.fail(CliError(ended, 1)))(answer)

  /** [[ask]] for a question that takes any answer. */
  def line(label: String, ended: String): Task[String] =
    ask(label, ended)(ZIO.right(_))

  /** One line from stdin, or `None` once input has ended. */
  val readLine: Task[Option[String]] =
    Console.readLine.asSome.catchSome { case _: EOFException => ZIO.none }
}
