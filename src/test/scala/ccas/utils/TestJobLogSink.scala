package ccas.utils

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime
import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.jdk.CollectionConverters.*

import zio.{Chunk, Ref, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

object TestJobLogSink extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobLogSink")(
    testStdoutSinkIsDefault,
    testFileSinkAppendsLines,
    testFileSinkStripsAnsiFromFile,
    testFileSinkTeesAnsiToStdout,
    testFileSinkCloseFlushesAndLatches,
    testFileSinkOpenFailureDisablesFile,
    testCurrentSinkLocallyOverride,
    testSweepBeforeDeletesOldLogsOnly
  ) @@ TestAspect.sequential

  // Built at runtime to avoid embedding a raw ESC byte in the source file.
  private val Esc      = Character.toString(0x1B)
  private val AnsiLine = s"${Esc}[32m[INFO 00:00:00]${Esc}[0m hi"

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

  private def tempLogDir: ZIO[Any, Throwable, Path] =
    ZIO.attempt(Files.createTempDirectory("ccas-job-log-sink-test"))

  private def testStdoutSinkIsDefault = test("currentSink defaults to StdoutSink") {
    JobLogSink.currentSink.get.map(sink => assertTrue(sink eq JobLogSink.StdoutSink))
  }

  private def testFileSinkAppendsLines = test("FileSink.write appends each line as a separate line to its file") {
    captureStdout(
      for {
        dir  <- tempLogDir
        sink <- ccas.server.jobs.FileSink.make(dir, "job-append")
        _    <- ZIO.foreachDiscard(List("first", "second"))(sink.write)
        path =  dir.resolve("job-append.log")
        lines <- ZIO.attempt(Files.readAllLines(path).asScala.toList)
      } yield assertTrue(lines == List("first", "second"))
    ).map(_._1)
  }

  private def testFileSinkStripsAnsiFromFile = test("FileSink strips ANSI escapes from file content") {
    captureStdout(
      for {
        dir  <- tempLogDir
        sink <- ccas.server.jobs.FileSink.make(dir, "job-ansi-file")
        _    <- ZIO.succeed(sink.writeFileSync(AnsiLine))
        path =  dir.resolve("job-ansi-file.log")
        bytes <- ZIO.attempt(Files.readAllBytes(path))
        text  =  new String(bytes, StandardCharsets.UTF_8)
      } yield assertTrue(
        !bytes.contains(0x1B.toByte),
        text.trim == "[INFO 00:00:00] hi"
      )
    ).map(_._1)
  }

  private def testFileSinkTeesAnsiToStdout = test("FileSink tees the ANSI-preserved line to stdout") {
    captureStdout(
      for {
        dir  <- tempLogDir
        sink <- ccas.server.jobs.FileSink.make(dir, "job-ansi-stdout")
        _    <- ZIO.succeed(sink.writeConsoleSync(AnsiLine))
      } yield ()
    ).map { case (_, out) =>
      assertTrue(out.contains(AnsiLine))
    }
  }

  private def testFileSinkCloseFlushesAndLatches =
    test("close flushes buffered content and a post-close writeFileSync is a no-op") {
      captureStdout(
        for {
          dir   <- tempLogDir
          sink  <- ccas.server.jobs.FileSink.make(dir, "job-close")
          _     <- ZIO.succeed(sink.writeFileSync("before-close"))
          _     <- sink.close()
          _     <- ZIO.succeed(sink.writeFileSync("after-close")) // latched → dropped, must not throw
          path  =  dir.resolve("job-close.log")
          lines <- ZIO.attempt(Files.readAllLines(path).asScala.toList)
        } yield assertTrue(lines == List("before-close"))
      ).map(_._1)
    }

  private def testFileSinkOpenFailureDisablesFile =
    test("make tolerates an open failure: file logging disabled, stdout tee still works, no throw") {
      captureStdout(
        for {
          base       <- tempLogDir
          missing    =  base.resolve("does-not-exist") // parent dir absent → newBufferedWriter(CREATE) fails
          sink       <- ccas.server.jobs.FileSink.make(missing, "job-nodir")
          _          <- ZIO.succeed(sink.writeFileSync("dropped"))   // file disabled → no-op, must not throw
          _          <- ZIO.succeed(sink.writeConsoleSync("teed"))   // tee still works
          fileAbsent <- ZIO.attempt(!Files.exists(missing.resolve("job-nodir.log")))
        } yield fileAbsent
      ).map { case (fileAbsent, out) =>
        assertTrue(fileAbsent, out.contains("teed"))
      }
    }

  private def testSweepBeforeDeletesOldLogsOnly =
    test("sweepBefore deletes *.log files older than cutoff, keeps fresh logs and non-log files") {
      for {
        dir     <- tempLogDir
        oldLog   = dir.resolve("old.log")
        freshLog = dir.resolve("fresh.log")
        keepTxt  = dir.resolve("keep.txt") // old, but not a *.log → must survive the glob
        cutoff   = Instant.now()
        old      = FileTime.from(cutoff.minus(2, ChronoUnit.DAYS))
        fresh    = FileTime.from(cutoff.plus(1, ChronoUnit.DAYS)) // pin ahead of cutoff so mtime granularity can't flip it
        _ <- ZIO.attempt {
          Files.write(oldLog, "old".getBytes(StandardCharsets.UTF_8))
          Files.write(freshLog, "fresh".getBytes(StandardCharsets.UTF_8))
          Files.write(keepTxt, "keep".getBytes(StandardCharsets.UTF_8))
          Files.setLastModifiedTime(oldLog, old)
          Files.setLastModifiedTime(freshLog, fresh)
          Files.setLastModifiedTime(keepTxt, old)
        }
        deleted <- ccas.server.jobs.FileSink.sweepBefore(dir, cutoff)
      } yield assertTrue(
        deleted == 1,
        !Files.exists(oldLog),
        Files.exists(freshLog),
        Files.exists(keepTxt)
      )
    }

  /** Verifies the wiring path that JobRunner relies on: a `currentSink.locally(capture)` block around an effect that
    * fires `ZIO.logInfo` must route the formatted line through `capture` (because `ProgressDisplay.asZLogger` reads
    * the sink from the ZLogger context map).
    */
  private def testCurrentSinkLocallyOverride =
    test("currentSink.locally routes ProgressDisplay log lines through the override") {
      for {
        captured <- Ref.make(Chunk.empty[String])
        capture = new JobLogSink {
          // Test-only pattern: drive a ZIO Ref update from inside a sync callback via Runtime.default. Works in tests
          // because no production code is running under Runtime.default; do not reuse for real sinks.
          override def writeConsoleSync(line: String): Unit =
            zio.Unsafe.unsafe(implicit u =>
              zio.Runtime.default.unsafe.run(captured.update(_ :+ line)).getOrThrow()
            )
        }
        _ <- ZIO.scoped {
          ProgressDisplay.live(showProgress = false).build *>
            JobLogSink.currentSink.locally(capture)(ZIO.logInfo("payload"))
        }
        lines <- captured.get
      } yield assertTrue(
        lines.size == 1,
        lines.head.contains("payload"),
        lines.head.contains("[INFO")
      )
    }
}
