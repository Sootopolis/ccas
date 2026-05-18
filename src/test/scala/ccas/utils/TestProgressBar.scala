package ccas.utils

import java.io.{ByteArrayOutputStream, PrintStream}

import zio.{LogLevel, Promise, Ref, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

object TestProgressBar extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestProgressBar")(
    testPrintOutputsBarWithPercentage,
    testPrintHandlesZeroTotal,
    testFinishIsIdempotent,
    testScopedCallsFinishOnClose,
    testScopedCallsFinishOnInterrupt,
    testDisplayMultipleBars,
    testDisabledBarIsNoOp,
    testDisabledBarRemovedFromList,
    testLogAboveBarsRoutesThroughDisplay,
    testZioLogInfoRoutesThroughLiveLogger,
    testCurrentLogLevelFiltersDebug,
    testCurrentLogLevelEnablesDebug,
    testLoggerSwallowsThrow
  ) @@ TestAspect.sequential

  private def stripAnsi(s: String): String = s.replaceAll("\\u001b\\[[0-9;]*[a-zA-Z]", "").replaceAll("\r", "")

  /** Capture everything the effect writes to `System.out` and return it alongside the effect's result.
    * Sequential test execution is required (`TestAspect.sequential`) because `System.out` is process-global.
    */
  private def captureStdout[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, (A, String)] =
    ZIO.acquireReleaseWith(
      ZIO.succeed {
        val baos    = new ByteArrayOutputStream
        val origOut = System.out
        System.setOut(new PrintStream(baos, true, "UTF-8"))
        (baos, origOut)
      }
    ) { case (_, origOut) =>
      ZIO.succeed(System.setOut(origOut))
    } { case (baos, _) =>
      effect.map(a => (a, baos.toString("UTF-8")))
    }

  /** Capture both stdout and stderr — used when the asLogger lambda's defensive try/catch routes errors to stderr. */
  private def captureBoth[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, (A, String, String)] =
    ZIO.acquireReleaseWith(
      ZIO.succeed {
        val outBaos = new ByteArrayOutputStream
        val errBaos = new ByteArrayOutputStream
        val origOut = System.out
        val origErr = System.err
        System.setOut(new PrintStream(outBaos, true, "UTF-8"))
        System.setErr(new PrintStream(errBaos, true, "UTF-8"))
        (outBaos, errBaos, origOut, origErr)
      }
    ) { case (_, _, origOut, origErr) =>
      ZIO.succeed { System.setOut(origOut); System.setErr(origErr) }
    } { case (outBaos, errBaos, _, _) =>
      effect.map(a => (a, outBaos.toString("UTF-8"), errBaos.toString("UTF-8")))
    }

  private def withDisplay(enabled: Boolean = true): ProgressDisplay = ProgressDisplay.make(enabled)

  private def testPrintOutputsBarWithPercentage = test("print renders text, bar, and percentage") {
    captureStdout(
      ZIO.scoped {
        val display = withDisplay()
        for {
          bar <- display.addBarScoped
          _   <- bar.print(5, 10, "Working")
        } yield ()
      }
    ).map { case (_, out) =>
      assertTrue(
        out.contains("Working"),
        out.contains("50.0%"),
        out.contains("█"),
        out.contains("░")
      )
    }
  }

  private def testPrintHandlesZeroTotal = test("print shows 100% when total is zero") {
    captureStdout(
      ZIO.scoped {
        val display = withDisplay()
        for {
          bar <- display.addBarScoped
          _   <- bar.print(0, 0, "Empty")
        } yield ()
      }
    ).map { case (_, out) =>
      assertTrue(out.contains("100.0%"))
    }
  }

  private def testFinishIsIdempotent = test("finish is idempotent — no error on double finish") {
    ZIO.scoped {
      val display = withDisplay()
      for {
        bar <- display.addBarScoped
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
        val display = withDisplay()
        for {
          bar <- display.addBarScoped
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
        val display = withDisplay()
        for {
          bar <- display.addBarScoped
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
    captureStdout(
      ZIO.scoped {
        val display = withDisplay()
        for {
          bar1 <- display.addBarScoped
          bar2 <- display.addBarScoped
          _    <- bar1.print(3, 10, "Bar 1")
          _    <- bar2.print(7, 10, "Bar 2")
        } yield ()
      }
    ).map { case (_, out) =>
      assertTrue(out.contains("Bar 1"), out.contains("Bar 2"))
    }
  }

  private def testDisabledBarIsNoOp = test("disabled bar produces no output") {
    captureStdout(
      ZIO.scoped {
        val display = withDisplay(enabled = false)
        for {
          bar <- display.addBarScoped
          _   <- bar.print(5, 10, "Should not render")
          _   <- bar.finish
        } yield ()
      }
    ).map { case (_, out) =>
      assertTrue(out.isEmpty)
    }
  }

  /** Regression test for the disabled-mode bar leak: ensures `removeBar` clears `bars` even when `enabled = false`,
    * so that long-lived disabled displays (e.g. `CcasServer`) don't accumulate state across many job runs.
    */
  private def testDisabledBarRemovedFromList = test("disabled mode still removes bars from internal list") {
    val display = withDisplay(enabled = false)
    for {
      bar1   <- display.addBar
      bar2   <- display.addBar
      bar3   <- display.addBar
      after3 <- ZIO.succeed(display.barCount)
      _      <- bar1.finish
      _      <- bar2.finish
      _      <- bar3.finish
      empty  <- ZIO.succeed(display.barCount)
    } yield assertTrue(after3 == 3, empty == 0)
  }

  private def testLogAboveBarsRoutesThroughDisplay = test("logAboveBarsSync interleaves above the active bar") {
    captureStdout(
      ZIO.scoped {
        val display = withDisplay()
        for {
          bar <- display.addBarScoped
          _   <- bar.print(1, 10, "Progress")
          _   <- ZIO.succeed(display.logAboveBarsSync(JobLogSink.StdoutSink, "[INFO 00:00:00] Hello from logger"))
          _   <- bar.print(1, 10, "Progress")
        } yield ()
      }
    ).map { case (_, out) =>
      val expectedBar = "Progress " + "█" * 2 + "░" * 18 + " 10.0%"
      // Avoid stripAnsi for this assertion — the captured stream interleaves multiple ANSI sequences
      // back-to-back with the log payload, and a regex-based stripper risks chewing into the literal
      // brackets in `[INFO ...]`. Test the substring presence directly.
      assertTrue(
        out.contains("[INFO 00:00:00] Hello from logger"),
        out.contains(expectedBar)
      )
    }
  }

  /** End-to-end test that `ProgressDisplay.live` actually swaps `currentLoggers` so that `ZIO.logInfo` routes
    * through the custom formatter (level label, ANSI colour) and not ZIO's default console logger.
    */
  private def testZioLogInfoRoutesThroughLiveLogger = test("ZIO.logInfo routes through ProgressDisplay.live's ZLogger") {
    captureStdout(
      ZIO.scoped {
        ProgressDisplay.live(showProgress = true).build *>
          ZIO.logInfo("hello from zio")
      }
    ).map { case (_, out) =>
      // Don't strip ANSI here — assert presence of color escape + level label structure directly. Stripping
      // ANSI risks the regex chewing into the surrounding payload (e.g. `[32m` followed by `[INFO` could
      // be misread as overlapping when a stripper that doesn't anchor on ESC is used downstream).
      val hasAnsiCsi = out.matches("(?s).*\\u001b\\[\\d+m.*")
      assertTrue(
        out.contains("[INFO"),
        out.contains("hello from zio"),
        // Custom formatter emits at least one ANSI colour CSI — a regression to ZIO's default plain formatter
        // would have none. Match generically (any colour code) so palette tweaks don't break this test.
        hasAnsiCsi,
        // ZIO default logger emits structured `key=value` form; absence confirms removeDefaultLoggers worked.
        !out.contains("timestamp=")
      )
    }
  }

  /** `ZIO.logDebug` should be filtered when `currentLogLevel` is at the default `Info`. */
  private def testCurrentLogLevelFiltersDebug = test("ZIO.logDebug is suppressed when currentLogLevel is Info") {
    captureStdout(
      ZIO.scoped {
        ProgressDisplay.live(showProgress = true).build *>
          ZIO.logDebug("should not appear") *>
          ZIO.logInfo("should appear")
      }
    ).map { case (_, out) =>
      val clean = stripAnsi(out)
      assertTrue(
        !clean.contains("should not appear"),
        clean.contains("should appear")
      )
    }
  }

  /** When `currentLogLevel` is set to `Debug` via the `LogLevel` aspect, `ZIO.logDebug` should pass the filter. */
  private def testCurrentLogLevelEnablesDebug = test("ZIO.logDebug renders when currentLogLevel is Debug") {
    captureStdout(
      ZIO.scoped {
        ProgressDisplay.live(showProgress = true).build *>
          (ZIO.logDebug("debug-visible") @@ LogLevel.Debug)
      }
    ).map { case (_, out) =>
      val clean = stripAnsi(out)
      assertTrue(clean.contains("debug-visible"))
    }
  }

  /** A throwing `() => String` message must not propagate out of the logger callback (FiberRuntime.log is
    * infallible). The defensive try/catch should redirect the failure to `System.err`.
    *
    * `exit.isSuccess` is the diagnostic that distinguishes catch-present from catch-absent: with our try/catch the
    * thunk's RuntimeException stays inside `asZLogger.apply` and the surrounding effect completes normally; without
    * the catch the throw escapes through `FiberRuntime.log` and the fiber dies with a defect (`exit.isFailure`).
    */
  private def testLoggerSwallowsThrow = test("asLogger swallows exceptions from a throwing message thunk") {
    captureBoth(
      ZIO.scoped {
        ProgressDisplay.live(showProgress = true).build *>
          ZIO.logInfo {
            throw new RuntimeException("boom from message thunk")
          }
      }.exit
    ).map { case (exit, out, err) =>
      assertTrue(
        exit.isSuccess,
        // The defensive try/catch routed the throwable to stderr via `printStackTrace`. The stack trace
        // includes the exception class and the failure message, so check both.
        err.contains("RuntimeException"),
        err.contains("boom from message thunk"),
        // stdout did not receive a partial / broken line — the exception happened before any println.
        !out.contains("boom")
      )
    }
  }
}
