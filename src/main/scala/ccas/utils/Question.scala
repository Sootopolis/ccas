package ccas.utils

import zio.{IO, ZIO}

/** The one loop every question to the operator asks through (#514): it asks again for an answer it can't take, rather
  * than going ahead on a guess or giving up, and says why on a line of its own. A change to how a question asks again
  * goes here.
  */
object Question {

  /** Writes `shown` and reads lines through `readLine` (`None` once input has ended) until `answer` takes one, given
    * trimmed. Each it refuses writes its problem, indented, before asking again. Input that has ended is whatever
    * `ended` says, and an `answer` that fails ends the question with its failure.
    */
  def ask[E, A](shown: String, readLine: IO[E, Option[String]], write: String => IO[E, Unit], ended: IO[E, A])(
    answer: String => IO[E, Either[String, A]]
  ): IO[E, A] =
    for {
      _            <- write(shown)
      answerOption <- readLine
      answered <- answerOption.fold(ended) { typed =>
        answer(typed.trim).flatMap {
          case Right(taken)  => ZIO.succeed(taken)
          case Left(problem) => write(s"  $problem\n") *> ask(shown, readLine, write, ended)(answer)
        }
      }
    } yield answered
}
