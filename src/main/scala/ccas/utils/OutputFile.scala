package ccas.utils

import java.nio.file.{Files, NoSuchFileException, Path}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.util.Using

import zio.{Task, ZIO}

import ccas.api.misc.subtypes.ClubSlug

/** Where an app writes a report. Each write first moves the app's earlier output in that directory into `archive/`.
  * Club-scoped output (`write`, `writeAndLog`) goes to `<Root>/{clubSlug}/{timestamp}-{appName}.{ext}`, so one club's
  * history reads in order; output with no single club (`writeGlobal`, `writeAndLogGlobal`) goes to
  * `<Root>/{subDir}/{timestamp}-{appName}.{ext}`.
  */
object OutputFile {

  /** Where reports go: `output.directory` (`CCAS_OUTPUT_DIR`), else `$XDG_DATA_HOME/ccas/output`. */
  val Root: AppDir = AppDir(
    configKey = "output.directory",
    overrideVar = "CCAS_OUTPUT_DIR",
    base = AppDir.XdgBase.Data,
    leaf = "output"
  )

  private val dateTimeFormat = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")

  def write(appName: String, clubSlug: ClubSlug, content: String, ext: String = "txt"): Task[Path] =
    writeInternal(ClubSlug.unwrap(clubSlug), appName, content, ext)

  def writeAndLog(appName: String, clubSlug: ClubSlug, content: String, ext: String = "txt"): Task[Unit] =
    write(appName, clubSlug, content, ext).flatMap(path => ZIO.logInfo(s"Output written to $path"))

  def writeGlobal(appName: String, content: String, subDir: String, ext: String = "txt"): Task[Path] =
    writeInternal(subDir, appName, content, ext)

  // No warning for a relative root, unlike the job-log directory: the `writeAndLog` variants log each absolute path.
  private def writeInternal(subDir: String, appName: String, content: String, ext: String): Task[Path] =
    for {
      root <- Root.fromConfig
      dir  = root.toAbsolutePath.resolve(subDir)
      path = dir.resolve(s"${LocalDateTime.now().format(dateTimeFormat)}-$appName.$ext")
      _ <- ZIO.attemptBlocking(Files.createDirectories(dir))
      _ <- ZIO.attemptBlocking(archiveExisting(dir, appName, ext))
      _ <- ZIO.writeFile(path.toString, content)
    } yield path

  def writeAndLogGlobal(appName: String, content: String, subDir: String, ext: String = "txt"): Task[Unit] =
    writeGlobal(appName, content, subDir, ext).flatMap(path => ZIO.logInfo(s"Output written to $path"))

  private def archiveExisting(clubDir: Path, appName: String, ext: String): Unit =
    if (Files.exists(clubDir)) {
      val suffix     = s"-$appName.$ext"
      val archiveDir = clubDir.resolve("archive")
      Using(Files.list(clubDir)) { stream =>
        stream
          .filter(p => Files.isRegularFile(p) && p.getFileName.toString.endsWith(suffix))
          .forEach { p =>
            Files.createDirectories(archiveDir)
            // A concurrent write to the same dir may have already moved this file; tolerate the race.
            try { Files.move(p, archiveDir.resolve(p.getFileName)): Unit }
            catch { case _: NoSuchFileException => () }
          }
      }: Unit
    }
}
