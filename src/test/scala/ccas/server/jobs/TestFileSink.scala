package ccas.server.jobs

import java.io.{BufferedWriter, ByteArrayOutputStream, IOException, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import scala.jdk.CollectionConverters.*

import zio.ZIO
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

/** Behavioural tests for [[FileSink]]'s write-failure handling (issue #52): suppress → periodic-retry → recover, the
  * dropped-line counter, and the resume / final-summary markers. Lives in `package ccas.server.jobs` so it can reach
  * the `private[jobs]` constructor and `droppedLineCount` accessor — the deterministic seam. Failure and recovery are
  * driven by injected writer thunks (no sleeps): the count-gate (`retryAfterLines`) is the deterministic trigger and
  * the time-gate is neutralised with `retryAfterNanos = Long.MaxValue` and a constant clock.
  */
object TestFileSink extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestFileSink")(
    testTransientOpenFailureRecovers,
    testMidJobWriteFailureRecovers,
    testCounterCountsEveryDropOnce,
    testFinalSummaryWhenReopenSucceeds,
    testFinalSummaryToStderrWhenReopenFails,
    testNoReopenOrWriteAfterClose
  ) @@ TestAspect.sequential

  private val NoTimeGate: Long = Long.MaxValue // retryAfterNanos so high the time-gate never fires
  private val NoLineGate: Long = Long.MaxValue // retryAfterLines so high the count-gate never fires
  private val ZeroClock        = () => 0L

  private def realOpen(path: Path): () => BufferedWriter =
    () => Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)

  private def tempDir: ZIO[Any, Throwable, Path] =
    ZIO.attempt(Files.createTempDirectory("ccas-filesink-test"))

  private def readLines(path: Path): ZIO[Any, Throwable, List[String]] =
    ZIO.attempt(Files.readAllLines(path).asScala.toList)

  private def captureStderr[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, (A, String)] =
    ZIO.acquireReleaseWith(
      ZIO.succeed {
        val baos    = new ByteArrayOutputStream
        val origErr = System.err
        System.setErr(new PrintStream(baos, true, "UTF-8"))
        (baos, origErr)
      }
    ) { case (_, origErr) =>
      ZIO.succeed(System.setErr(origErr))
    } { case (baos, _) =>
      effect.map(a => (a, baos.toString("UTF-8")))
    }

  // A BufferedWriter that delegates every write but throws on its `failAtFlush`-th flush, and never flushes on close —
  // so the line buffered when the flush throws is genuinely dropped (mirrors a real mid-write IO failure).
  private final class FlakyWriter(delegate: BufferedWriter, failAtFlush: Int) extends BufferedWriter(delegate) {
    private var flushes = 0
    override def write(s: String, off: Int, len: Int): Unit         = delegate.write(s, off, len)
    override def write(cbuf: Array[Char], off: Int, len: Int): Unit = delegate.write(cbuf, off, len)
    override def write(c: Int): Unit                                = delegate.write(c)
    override def flush(): Unit = {
      flushes += 1
      if (flushes == failAtFlush) throw new IOException("boom") else delegate.flush()
    }
    override def close(): Unit = ()
  }

  private def testTransientOpenFailureRecovers =
    test("an initial open failure is suppressed, then recovers once the dir reappears (resume marker + count)") {
      for {
        base <- tempDir
        dir   = base.resolve("sub") // not created → first reopen throws NoSuchFileException
        path  = dir.resolve("job.log")
        sink  = new FileSink(path, realOpen(path), None, ZeroClock, retryAfterLines = 1L, NoTimeGate)
        _    <- ZIO.succeed(sink.writeFileSync("dropped1")) // gate fires, reopen fails → 1 dropped
        _    <- ZIO.attempt(Files.createDirectories(dir))
        _    <- ZIO.succeed(sink.writeFileSync("recovered")) // gate fires, reopen succeeds
        lines <- readLines(path)
      } yield assertTrue(
        lines == List("[FileSink] file logging resumed after 1 dropped line(s)", "recovered"),
        sink.droppedLineCount == 1L
      )
    }

  private def testMidJobWriteFailureRecovers =
    test("a mid-job flush failure suppresses, then a reopen recovers; stderr logs the failure exactly once") {
      captureStderr(
        for {
          dir   <- tempDir
          path   = dir.resolve("job.log")
          flaky  = new FlakyWriter(realOpen(path)(), failAtFlush = 2)
          sink   = new FileSink(path, realOpen(path), Some(flaky), ZeroClock, retryAfterLines = 1L, NoTimeGate)
          _     <- ZIO.succeed(sink.writeFileSync("line1"))  // flush #1 ok
          _     <- ZIO.succeed(sink.writeFileSync("line2"))  // flush #2 throws → suppress, line2 dropped
          _     <- ZIO.succeed(sink.writeFileSync("line3"))  // gate fires, reopen → marker + line3
          lines <- readLines(path)
        } yield (lines, sink.droppedLineCount)
      ).map { case ((lines, dropped), err) =>
        assertTrue(
          lines == List("line1", "[FileSink] file logging resumed after 1 dropped line(s)", "line3"),
          dropped == 1L,
          err.split('\n').count(_.contains("[FileSink] write to")) == 1
        )
      }
    }

  private def testCounterCountsEveryDropOnce =
    test("every dropped line is counted exactly once when reopen keeps failing, and no file is created") {
      for {
        dir  <- tempDir
        path  = dir.resolve("job.log")
        bad   = () => throw new IOException("nope") // openWriter always fails
        sink  = new FileSink(path, bad, None, ZeroClock, retryAfterLines = 1L, NoTimeGate)
        _    <- ZIO.foreachDiscard(List("a", "b", "c", "d", "e"))(l => ZIO.succeed(sink.writeFileSync(l)))
        absent <- ZIO.attempt(!Files.exists(path))
      } yield assertTrue(sink.droppedLineCount == 5L, absent)
    }

  private def testFinalSummaryWhenReopenSucceeds =
    test("close records a dropped-line summary in the file when a reopen succeeds") {
      for {
        base <- tempDir
        dir   = base.resolve("sub") // absent during the dropped writes
        path  = dir.resolve("job.log")
        // Both gates disabled → writes just drop, no auto-reopen until close().
        sink  = new FileSink(path, realOpen(path), None, ZeroClock, retryAfterLines = NoLineGate, NoTimeGate)
        _    <- ZIO.foreachDiscard(List("x", "y", "z"))(l => ZIO.succeed(sink.writeFileSync(l)))
        _    <- ZIO.attempt(Files.createDirectories(dir))
        _    <- sink.close()
        lines <- readLines(path)
      } yield assertTrue(
        lines == List("[FileSink] 3 log line(s) dropped due to write failures"),
        sink.droppedLineCount == 3L
      )
    }

  private def testFinalSummaryToStderrWhenReopenFails =
    test("close falls back to stderr for the summary when reopen fails, and is idempotent") {
      captureStderr(
        for {
          dir  <- tempDir
          path  = dir.resolve("job.log")
          bad   = () => throw new IOException("nope")
          sink  = new FileSink(path, bad, None, ZeroClock, retryAfterLines = NoLineGate, NoTimeGate)
          _    <- ZIO.foreachDiscard(List("p", "q"))(l => ZIO.succeed(sink.writeFileSync(l)))
          _    <- sink.close()
          _    <- sink.close() // idempotent → no second summary
          absent <- ZIO.attempt(!Files.exists(path))
        } yield (sink.droppedLineCount, absent)
      ).map { case ((dropped, absent), err) =>
        assertTrue(
          dropped == 2L,
          absent,
          err.split('\n').count(_.contains("2 log line(s) dropped")) == 1
        )
      }
    }

  private def testNoReopenOrWriteAfterClose =
    test("after close, writes are no-ops and never trigger a reopen") {
      for {
        dir  <- tempDir
        path  = dir.resolve("job.log")
        good  = realOpen(path)()
        trip  = () => throw new AssertionError("must not reopen after close")
        sink  = new FileSink(path, trip, Some(good), ZeroClock, retryAfterLines = 1L, NoTimeGate)
        _    <- ZIO.succeed(sink.writeFileSync("a"))
        _    <- sink.close()
        _    <- ZIO.succeed(sink.writeFileSync("b")) // closed → no-op, must not invoke `trip`
        lines <- readLines(path)
      } yield assertTrue(lines == List("a"), sink.droppedLineCount == 0L)
    }
}
