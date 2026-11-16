package ccas.cli

import java.io.EOFException

import zio.{Console, Task, UIO, ZIO}

/** The CLI's questions to the operator, read from stdin. */
private[cli] object CliPrompt {

  /** Prints `question` with `[Y/n]` and reads the answer: Enter, `y` or `yes` is yes, `n` or `no` is no, and anything
    * else asks again, so a typo neither goes ahead nor quietly declines. Input that has ended is no, so nothing goes
    * ahead unasked.
    */
  def askYes(question: String): UIO[Boolean] =
    for {
      _      <- Console.print(s"$question [Y/n] ").orDie
      answer <- Console.readLine.orElseSucceed("n")
      yes <- answer.trim.toLowerCase match {
        case "" | "y" | "yes" => ZIO.succeed(true)
        case "n" | "no"       => ZIO.succeed(false)
        case _                => Console.printLine("  enter y or n").orDie *> askYes(question)
      }
    } yield yes

  /** Prints `label` and reads one line, trimmed. Input that has ended fails with `ended`, which names what was left
    * unanswered, in place of the bare "error: null" an `EOFException` would print.
    */
  def line(label: String, ended: String): Task[String] =
    (Console.print(label) *> Console.readLine)
      .map(_.trim)
      .catchSome { case _: EOFException => ZIO.fail(CliError(ended, 1)) }
}
