package ccas.utils

import zio.{Ref, ZIO}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

/** The one rule every yes/no question reads its answer by ([[YesNo]]), and its asking on the console. */
object TestYesNo extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("TestYesNo")(
    test("Enter takes the default") {
      assertTrue(
        YesNo.read("", default = true) == Some(true),
        YesNo.read("  ", default = false) == Some(false)
      )
    },
    test("y, yes, n and no answer in any case, whatever the default") {
      val yeses = List("y", "Y", " yes ", "YES").map(YesNo.read(_, default = false))
      val noes  = List("n", "N", " no ", "No").map(YesNo.read(_, default = true))
      assertTrue(yeses.forall(_ == Some(true)), noes.forall(_ == Some(false)))
    },
    test("anything else asks again") {
      val answers = List("yep", "nope", "true", "1", "ye s").map(YesNo.read(_, default = true))
      assertTrue(answers.forall(_.isEmpty))
    },
    test("the hint shows the default in capitals") {
      assertTrue(
        YesNo.prompt("Save?", default = true) == "Save? [Y/n] ",
        YesNo.prompt("Stop?", default = false) == "Stop? [y/N] "
      )
    },
    test("ask: each answer read as the rule reads it, with the hint for the default") {
      val answers = List("", "y", " Yes ", "n", "No")
      for {
        _         <- TestConsole.feedLines(answers*)
        answered  <- ZIO.foreach(answers)(_ => YesNo.ask("Go?", default = true))
        _         <- TestConsole.feedLines("")
        enterIsNo <- YesNo.ask("Stop?", default = false)
        output    <- TestConsole.output
      } yield assertTrue(
        answered == List(true, true, true, false, false),
        !enterIsNo,
        output.head == "Go? [Y/n] ",
        output.last == "Stop? [y/N] "
      )
    },
    test("ask: any other answer asks again, neither going ahead nor declining") {
      for {
        _      <- TestConsole.feedLines("yeah", "6", "y")
        yes    <- YesNo.ask("Go?", default = true)
        output <- TestConsole.output
        _      <- TestConsole.feedLines("nope", "n")
        no     <- YesNo.ask("Go?", default = true)
      } yield assertTrue(
        yes,
        !no,
        output.mkString == "Go? [Y/n]   enter y or n\nGo? [Y/n]   enter y or n\nGo? [Y/n] "
      )
    },
    test("askWith: writes the prompt it is given, and asks again through the same writer") {
      for {
        lines   <- Ref.make(List("yeah", "n"))
        written <- Ref.make("")
        no <- YesNo.askWith(
          shown = "keep? ",
          default = true,
          readLine = lines.modify(remaining => (remaining.headOption, remaining.drop(1))),
          write = text => written.update(_ + text),
          ended = ZIO.succeed(true)
        )
        output <- written.get
      } yield assertTrue(!no, output == "keep?   enter y or n\nkeep? ")
    },
    test("askOr: input that has ended is what the caller says it is") {
      for {
        exit <- YesNo.askOr("Go?", default = true, ZIO.fail("ended")).exit
      } yield assertTrue(exit.causeOption.flatMap(_.failureOption).contains("ended"))
    },
    test("ask: input that has ended is no, even where Enter would be yes") {
      for {
        yes <- YesNo.ask("Go?", default = true)
      } yield assertTrue(!yes)
    }
  )
}
