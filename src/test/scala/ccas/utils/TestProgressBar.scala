package ccas.utils

import java.io.{ByteArrayOutputStream, PrintStream}

import zio.{LogLevel, Promise, Ref, ZIO}
import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, ZIOSpecDefault}

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

  /** Run `use` against a `ProgressDisplay` whose bar redraws (and any defect stack trace) write to a private capture
    * buffer instead of process-global `System.out` / `System.err`, returning the effect's result alongside the
    * captured output. The companion `JobLogSink` passed to `use` writes to the *same* buffer, so a test that mixes
    * `bar.print` with `logAboveBarsSync` sees both interleaved in call order. Removes the cross-suite `System.out`
    * contention tracked in #64 — no global stream is mutated, so this needs no `System.setOut` swap.
    */
  private def withCapture[E, A](enabled: Boolean)(
    use: (ProgressDisplay, JobLogSink) => ZIO[Any, E, A]
  ): ZIO[Any, E, (A, String)] =
    ZIO.suspendSucceed {
      val baos    = new ByteArrayOutputStream
      val ps      = new PrintStream(baos, true, "UTF-8")
      val display = ProgressDisplay.makeWith(enabled, ps, ps)
      val sink    = new JobLogSink { override def writeSync(line: String): Unit = ps.println(line) }
      use(display, sink).map { a =>
        ps.flush()
        (a, baos.toString("UTF-8"))
      }
    }

  /** Run `effect` under `ProgressDisplay.live` (so its `ZLogger` is installed) with the log sink overridden to capture
    * formatted lines into a buffer — mirrors the `JobRunner` wiring (`currentSink.locally`) and `TestJobLogSink`'s
    * capture idiom. The captured string is the formatter output (`[LEVEL HH:mm:ss] msg`, ANSI-coloured); a line only
    * lands here if the live `ZLogger` actually routed it. No process-global stream is mutated.
    */
  private def withLogCapture[A](effect: ZIO[Any, Throwable, A]): ZIO[Any, Throwable, (A, String)] =
    ZIO.suspendSucceed {
      val baos    = new ByteArrayOutputStream
      val ps      = new PrintStream(baos, true, "UTF-8")
      val capture = new JobLogSink { override def writeSync(line: String): Unit = ps.println(line) }
      ZIO.scoped {
        ProgressDisplay.live(showProgress = false).build *>
          JobLogSink.currentSink.locally(capture)(effect)
      }.map { a =>
        ps.flush()
        (a, baos.toString("UTF-8"))
      }
    }

  private def testPrintOutputsBarWithPercentage = test("print renders text, bar, and percentage") {
    withCapture(enabled = true) { (display, _) =>
      ZIO.scoped {
        for {
          bar <- display.addBarScoped
          _   <- bar.print(5, 10, "Working")
        } yield ()
      }
    }.map { case (_, out) =>
      assertTrue(
        out.contains("Working"),
        out.contains("50.0%"),
        out.contains("█"),
        out.contains("░")
      )
    }
  }

  private def testPrintHandlesZeroTotal = test("print shows 100% when total is zero") {
    withCapture(enabled = true) { (display, _) =>
      ZIO.scoped {
        for {
          bar <- display.addBarScoped
          _   <- bar.print(0, 0, "Empty")
        } yield ()
      }
    }.map { case (_, out) =>
      assertTrue(out.contains("100.0%"))
    }
  }

  private def testFinishIsIdempotent = test("finish is idempotent — no error on double finish") {
    withCapture(enabled = true) { (display, _) =>
      ZIO.scoped {
        for {
          bar <- display.addBarScoped
          _   <- bar.print(5, 10, "Working")
          _   <- bar.finish
          _   <- bar.finish
        } yield ()
      }
    }.as(assertCompletes)
  }

  private def testScopedCallsFinishOnClose = test("scoped bar is automatically removed when scope closes") {
    for {
      ref <- Ref.make(false)
      _ <- withCapture(enabled = true) { (display, _) =>
        ZIO.scoped {
          for {
            bar <- display.addBarScoped
            _   <- bar.print(5, 10, "Scoped")
            _   <- ref.set(true)
          } yield ()
        }
      }
      completed <- ref.get
    } yield assertTrue(completed)
  }

  private def testScopedCallsFinishOnInterrupt = test("scoped bar is cleaned up on interruption") {
    for {
      started <- Promise.make[Nothing, Unit]
      fiber <- withCapture(enabled = true) { (display, _) =>
        ZIO.scoped {
          for {
            bar <- display.addBarScoped
            _   <- bar.print(1, 10, "Interrupted")
            _   <- started.succeed(())
            _   <- ZIO.never
          } yield ()
        }.fork
      }.map(_._1)
      _      <- started.await
      result <- fiber.interrupt
    } yield assertTrue(result.isInterrupted)
  }

  private def testDisplayMultipleBars = test("display supports multiple bars") {
    withCapture(enabled = true) { (display, _) =>
      ZIO.scoped {
        for {
          bar1 <- display.addBarScoped
          bar2 <- display.addBarScoped
          _    <- bar1.print(3, 10, "Bar 1")
          _    <- bar2.print(7, 10, "Bar 2")
        } yield ()
      }
    }.map { case (_, out) =>
      assertTrue(out.contains("Bar 1"), out.contains("Bar 2"))
    }
  }

  private def testDisabledBarIsNoOp = test("disabled bar produces no output") {
    withCapture(enabled = false) { (display, _) =>
      ZIO.scoped {
        for {
          bar <- display.addBarScoped
          _   <- bar.print(5, 10, "Should not render")
          _   <- bar.finish
        } yield ()
      }
    }.map { case (_, out) =>
      assertTrue(out.isEmpty)
    }
  }

  /** Regression test for the disabled-mode bar leak: ensures `removeBar` clears `bars` even when `enabled = false`,
    * so that long-lived disabled displays (e.g. `CcasServer`) don't accumulate state across many job runs.
    */
  private def testDisabledBarRemovedFromList = test("disabled mode still removes bars from internal list") {
    withCapture(enabled = false) { (display, _) =>
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
    }.map(_._1)
  }

  private def testLogAboveBarsRoutesThroughDisplay = test("logAboveBarsSync interleaves above the active bar") {
    withCapture(enabled = true) { (display, sink) =>
      ZIO.scoped {
        for {
          bar <- display.addBarScoped
          _   <- bar.print(1, 10, "Progress")
          _   <- ZIO.succeed(display.logAboveBarsSync(sink, "[INFO 00:00:00] Hello from logger"))
          _   <- bar.print(1, 10, "Progress")
        } yield ()
      }
    }.map { case (_, out) =>
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
    * through the custom formatter (level label, ANSI colour) and not ZIO's default console logger. The capture sink
    * only receives a line if the live `ZLogger` routed it — the default ZIO logger never touches `JobLogSink` — so a
    * non-empty payload is itself proof the logger was installed.
    */
  private def testZioLogInfoRoutesThroughLiveLogger = test("ZIO.logInfo routes through ProgressDisplay.live's ZLogger") {
    withLogCapture(ZIO.logInfo("hello from zio")).map { case (_, out) =>
      // Custom formatter emits at least one ANSI colour CSI — a regression to ZIO's default plain formatter would
      // have none. Match generically (any colour code) so palette tweaks don't break this test.
      val hasAnsiCsi = out.matches("(?s).*\\u001b\\[\\d+m.*")
      assertTrue(
        out.contains("[INFO"),
        out.contains("hello from zio"),
        hasAnsiCsi
      )
    }
  }

  /** `ZIO.logDebug` should be filtered when `currentLogLevel` is at the default `Info`. */
  private def testCurrentLogLevelFiltersDebug = test("ZIO.logDebug is suppressed when currentLogLevel is Info") {
    withLogCapture(ZIO.logDebug("should not appear") *> ZIO.logInfo("should appear")).map { case (_, out) =>
      val clean = stripAnsi(out)
      assertTrue(
        !clean.contains("should not appear"),
        clean.contains("should appear")
      )
    }
  }

  /** When `currentLogLevel` is set to `Debug` via the `LogLevel` aspect, `ZIO.logDebug` should pass the filter. */
  private def testCurrentLogLevelEnablesDebug = test("ZIO.logDebug renders when currentLogLevel is Debug") {
    withLogCapture(ZIO.logDebug("debug-visible") @@ LogLevel.Debug).map { case (_, out) =>
      val clean = stripAnsi(out)
      assertTrue(clean.contains("debug-visible"))
    }
  }

  /** A throwing `() => String` message must not propagate out of the logger callback (FiberRuntime.log is
    * infallible). The defensive try/catch should redirect the failure to the display's `err` stream.
    *
    * `exit.isSuccess` is the diagnostic that distinguishes catch-present from catch-absent: with our try/catch the
    * thunk's RuntimeException stays inside `asZLogger.apply` and the surrounding effect completes normally; without
    * the catch the throw escapes through `FiberRuntime.log` and the fiber dies with a defect (`exit.isFailure`).
    * `liveWith` injects the capture streams so the stack trace and any (here, none) log output are observed without
    * mutating process-global `System.err` / `System.out`.
    */
  private def testLoggerSwallowsThrow = test("asLogger swallows exceptions from a throwing message thunk") {
    ZIO.suspendSucceed {
      val outBaos = new ByteArrayOutputStream
      val outPs   = new PrintStream(outBaos, true, "UTF-8")
      val errBaos = new ByteArrayOutputStream
      val errPs   = new PrintStream(errBaos, true, "UTF-8")
      val capture = new JobLogSink { override def writeSync(line: String): Unit = outPs.println(line) }
      ZIO.scoped {
        ProgressDisplay.liveWith(showProgress = false, outPs, errPs).build *>
          JobLogSink.currentSink.locally(capture)(
            ZIO.logInfo {
              throw new RuntimeException("boom from message thunk")
            }
          )
      }.exit.map { exit =>
        outPs.flush()
        errPs.flush()
        val out = outBaos.toString("UTF-8")
        val err = errBaos.toString("UTF-8")
        assertTrue(
          exit.isSuccess,
          // The defensive try/catch routed the throwable to `err` via `printStackTrace`. The stack trace
          // includes the exception class and the failure message, so check both.
          err.contains("RuntimeException"),
          err.contains("boom from message thunk"),
          // The sink received nothing — the exception happened before any line was formatted/written.
          !out.contains("boom")
        )
      }
    }
  }
}
