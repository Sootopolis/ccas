package ccas.utils

import zio.{Ref, ZIO}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

object TestProgressBar extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestProgressBar")(
    testPrintOutputsBarWithPercentage,
    testPrintHandlesZeroTotal,
    testPrintHandlesMultilineText,
    testFinishIsIdempotent,
    testScopedCallsFinishOnClose,
    testScopedCallsFinishOnInterrupt,
    testErasesOldLinesOnUpdate
  )

  private def testPrintOutputsBarWithPercentage = test("print renders text, bar, and percentage") {
    for {
      bar    <- ProgressBar.make
      _      <- bar.print(5, 10, "Working")
      output <- TestConsole.output
    } yield {
      val line = output.head
      assertTrue(
        line.contains("Working"),
        line.contains("50%"),
        line.contains("\u2588"),
        line.contains("\u2591"),
        line.startsWith("\r")
      )
    }
  }

  private def testPrintHandlesZeroTotal = test("print shows 100% when total is zero") {
    for {
      bar    <- ProgressBar.make
      _      <- bar.print(0, 0, "Empty")
      output <- TestConsole.output
    } yield assertTrue(output.head.contains("100%"))
  }

  private def testPrintHandlesMultilineText = test("print handles multiline text") {
    for {
      bar    <- ProgressBar.make
      _      <- bar.print(1, 2, "line1\nline2")
      output <- TestConsole.output
    } yield {
      val line = output.head
      assertTrue(line.contains("line1"), line.contains("line2"))
    }
  }

  private def testFinishIsIdempotent = test("finish is a no-op when nothing was printed") {
    for {
      bar    <- ProgressBar.make
      _      <- bar.finish
      _      <- bar.finish
      output <- TestConsole.output
    } yield assertTrue(output.isEmpty)
  }

  private def testScopedCallsFinishOnClose = test("scoped calls finish when scope closes") {
    for {
      _ <- ZIO.scoped {
        for {
          bar <- ProgressBar.scoped
          _   <- bar.print(5, 10, "Scoped")
        } yield ()
      }
      output <- TestConsole.output
    } yield {
      // Should have the progress print + a newline from finish
      assertTrue(output.size == 2, output.last == "\n")
    }
  }

  private def testScopedCallsFinishOnInterrupt = test("scoped calls finish on interruption") {
    // TestConsole output is unreliable for interrupted forked fibers, so we
    // verify the finalizer ran by checking that lastLineCount was reset to 0.
    for {
      ref <- Ref.make(Option.empty[ProgressBar])
      fiber <- ZIO.scoped {
        for {
          bar <- ProgressBar.scoped
          _   <- ref.set(Some(bar))
          _   <- bar.print(1, 10, "Interrupted")
          _   <- ZIO.never
        } yield ()
      }.fork
      _      <- fiber.interrupt
      barOpt <- ref.get
      // After interrupt + finish finalizer, calling finish again should be a no-op
      // (lastLineCount is already 0). We verify by checking no output is produced.
      _      <- TestConsole.clearOutput
      _      <- ZIO.foreachDiscard(barOpt)(_.finish)
      output <- TestConsole.output
    } yield assertTrue(output.isEmpty)
  }

  private def testErasesOldLinesOnUpdate = test("subsequent prints erase previous output") {
    for {
      bar    <- ProgressBar.make
      _      <- bar.print(1, 10, "First")
      _      <- bar.print(2, 10, "Second")
      output <- TestConsole.output
    } yield {
      val secondPrint = output(1)
      // Second print should contain ANSI escape to move up and erase the first line
      assertTrue(
        secondPrint.contains("\u001b[A"),
        secondPrint.contains("\u001b[2K"),
        secondPrint.contains("Second")
      )
    }
  }
}
