package ccas.utils

import scala.collection.mutable

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** The conversation [[TerminalQuestion]] holds, over scripted lines in place of `/dev/tty`, which a test can't open. */
object TestTerminalQuestion extends ZIOSpecDefault {

  /** The answer `lines` give, and everything written to the terminal on the way; an exhausted script is ended input. */
  private def converse(lines: String*): (Boolean, String) = {
    val remaining = mutable.Queue(lines*)
    val written   = new StringBuilder
    val yes = TerminalQuestion.converse(
      question = "Boot?",
      readLine = () => Option.when(remaining.nonEmpty)(remaining.dequeue()),
      write = text => written ++= text
    )
    (yes, written.toString)
  }

  override def spec: Spec[Any, Nothing] = suite("TestTerminalQuestion")(
    test("y or yes, in any case, goes ahead, and the hint shows the default is no") {
      val (yes, written) = converse(" YES ")
      assertTrue(yes, converse("y")._1, written == "Boot? [y/N] ")
    },
    test("Enter, n or no is no, and says aborted") {
      val answers = List("", "n", "No").map(converse(_))
      assertTrue(answers.forall(!_._1), answers.forall(_._2.endsWith("aborted\n")))
    },
    test("an answer that is neither yes nor no asks again, rather than aborting") {
      val (yes, written) = converse("yep", "y")
      assertTrue(yes, written == "Boot? [y/N]   enter y or n\nBoot? [y/N] ")
    },
    test("input that has ended is no") {
      val (yes, written) = converse()
      assertTrue(!yes, written == "Boot? [y/N] aborted\n")
    }
  )
}
