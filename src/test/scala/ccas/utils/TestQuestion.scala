package ccas.utils

import zio.{IO, Ref, UIO, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** The one loop every question asks through ([[Question.ask]]), over lines and a writer that stand in for a console. */
object TestQuestion extends ZIOSpecDefault {

  /** A conversation: `lines` are read in turn, then input ends; everything written is kept. */
  private final case class Conversation(lines: Ref[List[String]], written: Ref[String]) {
    val readLine: UIO[Option[String]]  = lines.modify(remaining => (remaining.headOption, remaining.drop(1)))
    def write(text: String): UIO[Unit] = written.update(_ + text)

    def ask[E, A](ended: IO[E, A])(answer: String => IO[E, Either[String, A]]): IO[E, A] =
      Question.ask("count? ", readLine, write, ended)(answer)
  }

  private def conversation(lines: String*): UIO[Conversation] =
    Ref.make(lines.toList).zipWith(Ref.make(""))(Conversation(_, _))

  private val whole: String => UIO[Either[String, Int]] =
    typed => ZIO.succeed(typed.toIntOption.toRight(s"'$typed' is not a whole number"))

  override def spec: Spec[Any, Any] = suite("TestQuestion")(
    test("an answer it refuses is asked again, with the problem indented on a line of its own") {
      for {
        talk    <- conversation("three", "", "3")
        count   <- talk.ask(ZIO.succeed(0))(whole)
        written <- talk.written.get
      } yield assertTrue(
        count == 3,
        written == "count?   'three' is not a whole number\ncount?   '' is not a whole number\ncount? "
      )
    },
    test("the answer is read from the line with its surrounding spaces trimmed") {
      for {
        talk  <- conversation("  12 \t")
        count <- talk.ask(ZIO.succeed(0))(whole)
      } yield assertTrue(count == 12)
    },
    test("input that has ended is what the caller says it is") {
      for {
        talk    <- conversation("x")
        exit    <- talk.ask(ZIO.fail("ended"))(whole).exit
        written <- talk.written.get
      } yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).contains("ended"),
        written == "count?   'x' is not a whole number\ncount? "
      )
    },
    test("an answer that fails ends the question rather than asking again") {
      for {
        talk      <- conversation("1", "2")
        exit      <- talk.ask(ZIO.succeed(0))(_ => ZIO.fail("lookup failed")).exit
        remaining <- talk.lines.get
      } yield assertTrue(exit.causeOption.flatMap(_.failureOption).contains("lookup failed"), remaining == List("2"))
    }
  )
}
