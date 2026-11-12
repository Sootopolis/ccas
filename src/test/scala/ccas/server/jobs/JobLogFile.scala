package ccas.server.jobs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import zio.{UIO, ZIO}

import ccas.utils.{JobLogSink, ProgressDisplay}

/** What an effect logs as a job ([[JobRunner.logAsJob]]), read back from the `.log` file a [[FileSink]] writes for it:
  * the lines the operator reads, not each message alone, which hid a bare `[INFO hh:mm:ss]` line from the membership
  * snapshots (#467). Each line's time reads `hh:mm:ss`, so a snapshot of it is stable.
  */
object JobLogFile {

  def of(effect: UIO[Any]): ZIO[Any, Throwable, String] =
    ZIO.scoped {
      for {
        dir  <- ZIO.acquireRelease(ZIO.attemptBlocking(Files.createTempDirectory("ccas-job-log-file")))(deleteQuietly)
        sink <- FileSink.make(dir, JobId)
        _ <- JobRunner
          .logAsJob(ProgressDisplay.make(enabled = false), JobId, fileOnly(sink))(effect)
          .ensuring(sink.close())
        text <- ZIO.attemptBlocking(Files.readString(dir.resolve(s"$JobId.log"), StandardCharsets.UTF_8))
      } yield LineTime.replaceAllIn(text, "$1 hh:mm:ss]")
    }

  private val JobId = "job"

  // The level bracket that starts each line; a cause's continuation lines have none.
  private val LineTime = """(?m)^(\[\w+) \d{2}:\d{2}:\d{2}\]""".r

  // FileSink also tees each line to stdout, where a test's report would read as the run's own output.
  private def fileOnly(sink: FileSink): JobLogSink = new JobLogSink {
    override def writeConsoleSync(line: String): Unit = ()
    override def writeFileSync(line: String): Unit    = sink.writeFileSync(line)
  }

  private def deleteQuietly(dir: Path): UIO[Unit] =
    ZIO.attemptBlocking {
      Files.deleteIfExists(dir.resolve(s"$JobId.log"))
      Files.delete(dir)
    }.ignore
}
