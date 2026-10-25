package ccas.server.jobs

import java.nio.file.{Files, Paths}

import zio.{LogLevel, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault, ZTestLogger}

/** `JobRunner.prepareLogDir` alone, without the database `TestJobRunner` needs to build the whole layer. */
object TestJobLogDir extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("JobLogDir")(
    test("a relative job-logs.directory is pinned to an absolute path at boot, with a warning naming it") {
      val relative = Paths.get("target/test-job-log-dir/relative")
      val absolute = Paths.get("target/test-job-log-dir/absolute").toAbsolutePath
      for {
        pinned   <- JobRunner.prepareLogDir(relative)
        kept     <- JobRunner.prepareLogDir(absolute)
        warnings <- ZTestLogger.logOutput.map(_.filter(_.logLevel == LogLevel.Warning).map(_.message()))
        created  <- ZIO.attemptBlocking(Files.isDirectory(pinned) && Files.isDirectory(kept))
      } yield assertTrue(
        pinned == relative.toAbsolutePath,
        kept == absolute,
        created,
        // One warning, for the relative setting only.
        warnings.size == 1,
        warnings.exists(_.contains(pinned.toString))
      )
    }.provide(ZTestLogger.default),
    test("a job-log directory that can't be created fails the boot with a message naming JOB_LOGS_DIR") {
      val blocker = Paths.get("target/test-job-log-dir/blocker").toAbsolutePath
      for {
        _    <- ZIO.attemptBlocking(Files.createDirectories(blocker.getParent))
        _    <- ZIO.attemptBlocking(Files.deleteIfExists(blocker))
        _    <- ZIO.attemptBlocking(Files.createFile(blocker))
        exit <- JobRunner.prepareLogDir(blocker.resolve("job-logs")).exit
      } yield assertTrue(exit.causeOption.flatMap(_.failureOption).exists(_.getMessage.contains("JOB_LOGS_DIR")))
    }
  )
}
