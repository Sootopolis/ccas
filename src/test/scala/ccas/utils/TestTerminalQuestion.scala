package ccas.utils

import scala.collection.mutable

import zio.{UIO, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** The conversation [[TerminalQuestion]] holds, over scripted lines in place of `/dev/tty`, which a test can't open. */
object TestTerminalQuestion extends ZIOSpecDefault {

  /** The answer `lines` give, and everything written to the terminal on the way; an exhausted script is ended input. */
  private def converse(lines: String*): UIO[(Boolean, String)] = {
    val remaining = mutable.Queue(lines*)
    val written   = new StringBuilder
    TerminalQuestion
      .converse(
        question = "Boot?",
        readLine = ZIO.succeed(Option.when(remaining.nonEmpty)(remaining.dequeue())),
        write = text => ZIO.succeed(written ++= text).unit
      )
      .orDie
      .map(yes => (yes, written.toString))
  }

  override def spec: Spec[Any, Nothing] = suite("TestTerminalQuestion")(
    test("y or yes, in any case, goes ahead, and the hint shows the default is no") {
      for {
        (yes, written) <- converse(" YES ")
        (y, _)         <- converse("y")
      } yield assertTrue(yes, y, written == "Boot? [y/N] ")
    },
    test("Enter, n or no is no, and says aborted") {
      for {
        answers <- ZIO.foreach(List("", "n", "No"))(converse(_))
      } yield assertTrue(answers.forall(!_._1), answers.forall(_._2.endsWith("aborted\n")))
    },
    test("an answer that is neither yes nor no asks again, rather than aborting") {
      for {
        (yes, written) <- converse("yep", "y")
      } yield assertTrue(yes, written == "Boot? [y/N]   enter y or n\nBoot? [y/N] ")
    },
    test("a run stays on the asking thread however many times the question is asked again") {
      val threads = mutable.Set.empty[Thread]
      val step    = ZIO.attempt(threads += Thread.currentThread()).unit
      for {
        (asker, exit) <- ZIO.succeedBlocking {
          (Thread.currentThread(), TerminalQuestion.runOnThisThread(ZIO.foreachDiscard(1 to 100000)(_ => step)))
        }
      } yield assertTrue(exit.isSuccess, threads.toSet == Set(asker))
    },
    test("input that has ended is no") {
      for {
        (yes, written) <- converse()
      } yield assertTrue(!yes, written == "Boot? [y/N] aborted\n")
    }
  )
}
