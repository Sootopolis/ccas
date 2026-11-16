package ccas.cli

import zio.ZIO
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

/** [[CliPrompt]]'s readings of what the operator typed, through a [[TestConsole]]. */
object TestCliPrompt extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("CliPrompt")(
    test("askYes: Enter, y or yes is yes, and n or no is no") {
      val answers = List("", "  ", "y", " Yes ", "YES", "n", "No", " N ")
      for {
        _      <- TestConsole.feedLines(answers*)
        yeses  <- ZIO.foreach(answers)(_ => CliPrompt.askYes("Go?"))
        output <- TestConsole.output
      } yield assertTrue(
        yeses == List(true, true, true, true, true, false, false, false),
        output.head == "Go? [Y/n] "
      )
    },
    test("askYes: any other answer asks again, neither going ahead nor declining") {
      for {
        _      <- TestConsole.feedLines("yeah", "6", "y")
        yes    <- CliPrompt.askYes("Go?")
        output <- TestConsole.output
        _      <- TestConsole.feedLines("nope", "n")
        no     <- CliPrompt.askYes("Go?")
      } yield assertTrue(
        yes,
        !no,
        output.mkString == "Go? [Y/n]   enter y or n\nGo? [Y/n]   enter y or n\nGo? [Y/n] "
      )
    },
    test("askYes: input that has ended is no, so nothing goes ahead unasked") {
      for {
        yes <- CliPrompt.askYes("Go?")
      } yield assertTrue(!yes)
    },
    test("line: input that has ended fails with the message naming what was left unanswered") {
      for {
        exit <- CliPrompt.line("Name: ", ended = "input ended before the name").exit
      } yield assertTrue(exit.causeOption.flatMap(_.failureOption).contains(CliError("input ended before the name", 1)))
    }
  )
}
