package ccas.utils

import zio.{Console, IO, UIO, ZIO}

/** How every yes/no question reads its answer, wherever it reads it from: Enter takes the default, `y`/`yes` or
  * `n`/`no` answers in any case, and anything else asks again, so a typo neither goes ahead nor quietly declines.
  */
object YesNo {

  /** Asks `question` on the console until the answer is yes or no. Input that has ended is no, whatever the default,
    * so nothing goes ahead unasked.
    */
  def ask(question: String, default: Boolean): UIO[Boolean] = askOr(question, default, ZIO.succeed(false))

  /** [[ask]], for a command that treats input ending as something other than a no: `ended` says what it is instead. */
  def askOr[E](question: String, default: Boolean, ended: IO[E, Boolean]): IO[E, Boolean] =
    for {
      _            <- Console.print(prompt(question, default)).orDie
      answerOption <- Console.readLine.option
      yes <- answerOption.fold(ended) { typed =>
        read(typed, default) match {
          case Some(yes) => ZIO.succeed(yes)
          case None      => Console.printLine(AskAgain).orDie *> askOr(question, default, ended)
        }
      }
    } yield yes

  /** `question` followed by the hint that shows the default in capitals: `[Y/n]` or `[y/N]`. */
  def prompt(question: String, default: Boolean): String =
    if (default) { s"$question [Y/n] " }
    else { s"$question [y/N] " }

  /** The answer `typed` gives, or `None` when it gives none and the question should be asked again. */
  def read(typed: String, default: Boolean): Option[Boolean] =
    typed.trim.toLowerCase match {
      case ""          => Some(default)
      case "y" | "yes" => Some(true)
      case "n" | "no"  => Some(false)
      case _           => None
    }

  /** What a question prints when an answer gives none. */
  val AskAgain = "  enter y or n"
}
