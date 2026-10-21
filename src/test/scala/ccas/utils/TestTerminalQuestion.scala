package ccas.utils

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** How [[TerminalQuestion]] reads an answer. The terminal conversation itself is not reachable from a test. */
object TestTerminalQuestion extends ZIOSpecDefault {

  override def spec: Spec[Any, Nothing] = suite("TestTerminalQuestion")(
    test("only y or yes confirms; a blank answer or EOF aborts") {
      assertTrue(
        TerminalQuestion.accepts(Some("y")),
        TerminalQuestion.accepts(Some(" YES ")),
        !TerminalQuestion.accepts(Some("")),
        !TerminalQuestion.accepts(Some("n")),
        !TerminalQuestion.accepts(Some("yep")),
        !TerminalQuestion.accepts(None)
      )
    }
  )
}
