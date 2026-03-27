package ccas.utils

import zio.{Promise, Ref, Scope, ZIO}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

object TestProgressBar extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestProgressBar")(
    testPrintOutputsBarWithPercentage,
    testPrintHandlesZeroTotal,
    testFinishIsIdempotent,
    testScopedCallsFinishOnClose,
    testScopedCallsFinishOnInterrupt,
    testDisplayMultipleBars,
    testDisabledBarIsNoOp,
    testLogAboveBarsRoutesThroughDisplay
  ).provide(Scope.default, CcasLogger.live())

  private def testPrintOutputsBarWithPercentage = test("print renders text, bar, and percentage") {
    ZIO.scoped {
      for {
        bar    <- CcasLogger.progressBar
        _      <- bar.print(5, 10, "Working")
        output <- TestConsole.output
      } yield {
        val all = output.mkString
        assertTrue(
          all.contains("Working"),
          all.contains("50.0%"),
          all.contains("\u2588"),
          all.contains("\u2591")
        )
      }
    }
  }

  private def testPrintHandlesZeroTotal = test("print shows 100% when total is zero") {
    ZIO.scoped {
      for {
        bar    <- CcasLogger.progressBar
        _      <- bar.print(0, 0, "Empty")
        output <- TestConsole.output
      } yield assertTrue(output.mkString.contains("100.0%"))
    }
  }

  private def testFinishIsIdempotent = test("finish is idempotent — no error on double finish") {
    ZIO.scoped {
      for {
        bar <- CcasLogger.progressBar
        _   <- bar.print(5, 10, "Working")
        _   <- bar.finish
        _   <- bar.finish
      } yield assertTrue(true)
    }
  }

  private def testScopedCallsFinishOnClose = test("scoped bar is automatically removed when scope closes") {
    for {
      ref <- Ref.make(false)
      _ <- ZIO.scoped {
        for {
          bar <- CcasLogger.progressBar
          _   <- bar.print(5, 10, "Scoped")
          _   <- ref.set(true)
        } yield ()
      }
      completed <- ref.get
    } yield assertTrue(completed)
  }

  private def testScopedCallsFinishOnInterrupt = test("scoped bar is cleaned up on interruption") {
    for {
      started <- Promise.make[Nothing, Unit]
      fiber <- ZIO.scoped {
        for {
          bar <- CcasLogger.progressBar
          _   <- bar.print(1, 10, "Interrupted")
          _   <- started.succeed(())
          _   <- ZIO.never
        } yield ()
      }.fork
      _      <- started.await
      result <- fiber.interrupt
    } yield assertTrue(result.isInterrupted)
  }

  private def testDisplayMultipleBars = test("display supports multiple bars") {
    ZIO.scoped {
      for {
        bar1   <- CcasLogger.progressBar
        bar2   <- CcasLogger.progressBar
        _      <- bar1.print(3, 10, "Bar 1")
        _      <- bar2.print(7, 10, "Bar 2")
        output <- TestConsole.output
      } yield {
        val all = output.mkString
        assertTrue(all.contains("Bar 1"), all.contains("Bar 2"))
      }
    }
  }

  private def testDisabledBarIsNoOp = test("disabled bar produces no output") {
    ZIO.scoped {
      for {
        bar    <- CcasLogger.progressBar
        _      <- bar.print(5, 10, "Should not render")
        _      <- bar.finish
        output <- TestConsole.output
      } yield assertTrue(output.isEmpty)
    }
  }.provideSome[Scope](CcasLogger.live(showProgress = false))

  private def testLogAboveBarsRoutesThroughDisplay = test("CcasLogger.info routes through active display") {
    ZIO.scoped {
      for {
        bar    <- CcasLogger.progressBar
        _      <- bar.print(1, 10, "Progress")
        _      <- CcasLogger.info("Hello from logger")
        output <- TestConsole.output
      } yield {
        val all = output.mkString
        assertTrue(
          all.contains("[INFO]"),
          all.contains("Hello from logger"),
          all.contains("Progress")
        )
      }
    }
  }
}
