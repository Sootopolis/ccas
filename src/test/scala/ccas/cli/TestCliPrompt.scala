package ccas.cli

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** [[CliPrompt]]'s readings of what the operator typed, through a `TestConsole`. */
object TestCliPrompt extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("CliPrompt")(
    test("line: input that has ended fails with the message naming what was left unanswered") {
      for {
        exit <- CliPrompt.line("Name: ", ended = "input ended before the name").exit
      } yield assertTrue(exit.causeOption.flatMap(_.failureOption).contains(CliError("input ended before the name", 1)))
    }
  )
}
