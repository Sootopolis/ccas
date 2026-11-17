package ccas.utils

import zio.{Console, IO, UIO, ZIO}

/** How every yes/no question reads its answer, wherever it reads it from: Enter takes the default, `y`/`yes` or
  * `n`/`no` answers in any case, and anything else asks again, so a typo neither goes ahead nor quietly declines.
  * [[askWith]] is the yes/no case of [[Question.ask]]; every yes/no question goes through it.
  */
object YesNo {

  /** Asks `question` on the console until the answer is yes or no. Input that has ended is no, whatever the default,
    * so nothing goes ahead unasked.
    */
  def ask(question: String, default: Boolean): UIO[Boolean] = askOr(question, default, ZIO.succeed(false))

  /** [[ask]], for a command that treats input ending as something other than a no: `ended` says what it is instead. */
  def askOr[E](question: String, default: Boolean, ended: IO[E, Boolean]): IO[E, Boolean] =
    askWith(prompt(question, default), default, Console.readLine.option, Console.print(_).orDie, ended)

  /** Writes `shown` and reads answers through `readLine` (`None` once input has ended) until [[read]] finds one,
    * writing [[AskAgain]] after each that gives none. Input that has ended is whatever `ended` says.
    */
  def askWith[E](
    shown: String,
    default: Boolean,
    readLine: IO[E, Option[String]],
    write: String => IO[E, Unit],
    ended: IO[E, Boolean]
  ): IO[E, Boolean] =
    Question.ask(shown, readLine, write, ended)(typed => ZIO.succeed(read(typed, default).toRight(AskAgain)))

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

  /** The problem a question shows when an answer gives none. */
  val AskAgain = "enter y or n"
}
