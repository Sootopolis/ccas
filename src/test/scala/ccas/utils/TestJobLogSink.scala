package ccas.utils

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

import zio.{Chunk, Ref, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

object TestJobLogSink extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobLogSink")(
    testStdoutSinkIsDefault,
    testFileSinkAppendsLines,
    testFileSinkStripsAnsiFromFile,
    testFileSinkTeesAnsiToStdout,
    testCurrentSinkLocallyOverride
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

  private def testFileSinkAppendsLines = test("FileSink appends each writeSync as a separate line to its file") {
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
        _    <- ZIO.succeed(sink.writeSync(AnsiLine))
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
        _    <- ZIO.succeed(sink.writeSync(AnsiLine))
      } yield ()
    ).map { case (_, out) =>
      assertTrue(out.contains(AnsiLine))
    }
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
          override def writeSync(line: String): Unit =
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
