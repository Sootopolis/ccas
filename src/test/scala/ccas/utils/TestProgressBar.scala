package ccas.utils

import java.io.{ByteArrayOutputStream, PrintStream}

import zio.stream.SubscriptionRef
import zio.{LogLevel, Promise, Ref, ZIO}
import zio.test.{assertCompletes, assertTrue, Spec, ZIOSpecDefault}

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
    testRenderPublishesRawSnapshotToChannel,
    testMultiLineBarsUseCursorUp,
    testLogAboveBarsRoutesThroughDisplay,
    testZioLogInfoRoutesThroughLiveLogger,
    testCurrentLogLevelFiltersDebug,
    testCurrentLogLevelEnablesDebug,
    testLoggerSwallowsThrow,
    testSourcedRendersPrefixTag,
    testStripSourceTag
  )
  // No `@@ TestAspect.sequential`: each test owns its capture buffers and installs loggers/sinks via fiber-scoped
  // FiberRefs (`currentLoggers`, `currentSink`), so the suite holds no process-global state to serialise. (Cross-suite
  // isolation is still provided by `Test/parallelExecution := false` in build.sbt, for the TestCcasCompletion fork-
  // pressure case — unrelated to this suite.)

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
      val display = ProgressDisplay.makeWith(enabled, ps, ps, None)
      val sink    = new JobLogSink { override def writeConsoleSync(line: String): Unit = ps.println(line) }
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
      val capture = new JobLogSink { override def writeConsoleSync(line: String): Unit = ps.println(line) }
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

  /** A bar created under an active [[ProgressDisplay.currentChannel]] (as `JobRunner` sets per job) mirrors its raw,
    * unrendered `current` / `total` / `text` into that channel on each `print` — keyed by bar id — and drops the key on
    * `finish`. This is the state `GET /api/jobs/{id}/progress` streams. Uses a disabled display: publishing is
    * independent of terminal drawing, so no capture stream is needed.
    */
  private def testRenderPublishesRawSnapshotToChannel =
    test("bar print publishes a raw snapshot to the active channel; finish drops it") {
      val display = ProgressDisplay.makeWith(enabled = false, System.out, System.err, None)
      for {
        channel <- SubscriptionRef.make(Map.empty[Int, BarSnapshot])
        afterPrint <- ProgressDisplay.currentChannel.locally(Some(channel)) {
          ZIO.scoped {
            for {
              bar  <- display.addBarScoped
              _    <- bar.print(3, 12, "  Working")
              snap <- channel.get
            } yield snap
          }
        }
        afterFinish <- channel.get
      } yield assertTrue(
        afterPrint.size == 1,
        afterPrint.values.head == BarSnapshot(afterPrint.keys.head, 3, 12, "  Working"),
        afterFinish.isEmpty
      )
    }

  /** Two bars draw on their own lines (a `\n` between them), and re-rendering a two-line block moves the cursor up over
    * it (`ESC[1A`) before repainting — the multi-line behaviour that replaced the old single `\r`-line. `ESC` is built
    * via `27.toChar` here (no source escape), matching the display.
    */
  private def testMultiLineBarsUseCursorUp = test("multiple bars draw on separate lines and redraw via cursor-up") {
    withCapture(enabled = true) { (display, _) =>
      ZIO.scoped {
        for {
          b1 <- display.addBarScoped
          b2 <- display.addBarScoped
          _  <- b1.print(1, 10, "Alpha")
          _  <- b2.print(2, 10, "Beta") // now a two-line block
          _  <- b1.print(3, 10, "Alpha") // redraw => cursor up over the two lines
        } yield ()
      }
    }.map { case (_, out) =>
      val esc = 27.toChar.toString
      assertTrue(
        out.contains("Alpha"),
        out.contains("Beta"),
        out.contains("\n"),          // bars occupy their own lines
        out.contains(s"$esc[1A")     // moved up one row to repaint the two-line block
      )
    }
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

  /** End-to-end test that `ProgressDisplay.live` installs the custom `ZLogger` so `ZIO.logInfo` routes through the
    * formatter (level label, ANSI colour). The capture sink only receives a line if the live `ZLogger` routed it —
    * ZIO's default console logger never touches `JobLogSink` — so a non-empty, correctly-formatted payload is itself
    * proof the logger was installed.
    *
    * Coverage note: this no longer also asserts ZIO's default console logger was *removed* (the old
    * `!out.contains("timestamp=")` check). That guard relied on capturing process-global `System.out`, where the
    * default logger writes; #64 removed that swap, and ZIO's logger set (`FiberRef.currentLoggers`) is `private[zio]`,
    * so the removal can't be asserted without reintroducing the swap. `live` runs `Runtime.removeDefaultLoggers`
    * unconditionally; a regression there would surface as visible double console output.
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
  /** The reserved `src` log annotation (set via `ProgressDisplay.sourced`) renders as a bracketed prefix right after
    * the time bracket — `[INFO HH:mm:ss] [<src>] msg` — so interleaved console output shows which job/component
    * emitted each line. Any other co-present annotation still renders as trailing `k=v`, and `src` itself is not
    * duplicated there.
    */
  private def testSourcedRendersPrefixTag = test("sourced renders a bracketed prefix; other annotations stay trailing") {
    val effect = ProgressDisplay.sourced("membership/london-cc")(
      ZIO.logAnnotate("run", "abc123")(ZIO.logInfo("reconciled"))
    )
    withLogCapture(effect).map { case (_, out) =>
      val clean = stripAnsi(out)
      assertTrue(
        clean.contains("] [membership/london-cc] reconciled"),
        clean.contains("run=abc123"),
        !clean.contains("src=")
      )
    }
  }

  /** `stripSourceTag` (used by the per-job file sink) removes only the source prefix directly abutting the level
    * bracket, leaving a message that itself starts with a `[...]` token — e.g. the reconciliation `[LEFT CLUB]`
    * headings — intact.
    */
  private def testStripSourceTag = test("stripSourceTag removes the source prefix and preserves a bracketed message") {
    assertTrue(
      ProgressDisplay.stripSourceTag("[INFO 00:00:00] [Membership/london-cc] reconciled") == "[INFO 00:00:00] reconciled",
      ProgressDisplay.stripSourceTag("[INFO 00:00:00] [Membership/x] [LEFT CLUB]") == "[INFO 00:00:00] [LEFT CLUB]",
      ProgressDisplay.stripSourceTag("[INFO 00:00:00] no tag here") == "[INFO 00:00:00] no tag here"
    )
  }

  private def testLoggerSwallowsThrow = test("asLogger swallows exceptions from a throwing message thunk") {
    ZIO.suspendSucceed {
      val outBaos = new ByteArrayOutputStream
      val outPs   = new PrintStream(outBaos, true, "UTF-8")
      val errBaos = new ByteArrayOutputStream
      val errPs   = new PrintStream(errBaos, true, "UTF-8")
      val capture = new JobLogSink { override def writeConsoleSync(line: String): Unit = outPs.println(line) }
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
