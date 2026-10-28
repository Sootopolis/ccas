package ccas.utils

import java.nio.file.{FileAlreadyExistsException, Files, NoSuchFileException, Path}
import java.nio.file.StandardOpenOption.CREATE_NEW
import java.time.format.DateTimeFormatter

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.util.Using

import zio.{Clock, Task, UIO, ZIO}

import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.errors.safeMessage

/** Where an app writes a report. Each write first moves the app's earlier output in that directory into `archive/`.
  * Club-scoped output (`write`, `writeAndLog`) goes to `<Root>/{clubSlug}/{timestamp}-{appName}.{ext}`, so one club's
  * history reads in order; output with no single club (`writeGlobal`, `writeAndLogGlobal`) goes to
  * `<Root>/{subDir}/{timestamp}-{appName}.{ext}`. A name already taken, there or in `archive/`, gains a counter
  * (`{timestamp}.1-{appName}.{ext}`) rather than replacing the file there.
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
      root  <- Root.fromConfig
      stamp <- Clock.localDateTime.map(_.format(dateTimeFormat))
      dir    = root.toAbsolutePath.resolve(subDir)
      suffix = s"-$appName.$ext"
      _    <- ZIO.attemptBlocking(Files.createDirectories(dir))
      _    <- archiveExisting(dir, suffix)
      path <- ZIO.attemptBlocking(claimFreeName(dir, stamp, suffix)(Files.writeString(_, content, CREATE_NEW)))
    } yield path

  def writeAndLogGlobal(appName: String, content: String, subDir: String, ext: String = "txt"): Task[Unit] =
    writeGlobal(appName, content, subDir, ext).flatMap(path => ZIO.logInfo(s"Output written to $path"))

  // A report that can't be archived is left in place with a warning: the write that follows takes a free name.
  private def archiveExisting(dir: Path, suffix: String): UIO[Unit] =
    ZIO
      .attemptBlocking(Using.resource(Files.list(dir))(_.iterator().asScala.filter(isReport(_, suffix)).toList))
      .foldZIO(
        failure = e => ZIO.logWarning(s"could not list earlier reports in $dir: ${e.safeMessage}"),
        success = reports => ZIO.foreachDiscard(reports)(archive(dir, suffix, _))
      )

  private def isReport(path: Path, suffix: String): Boolean =
    Files.isRegularFile(path) && path.getFileName.toString.endsWith(suffix)

  private def archive(dir: Path, suffix: String, report: Path): UIO[Unit] = {
    val archiveDir = dir.resolve("archive")
    val stem       = report.getFileName.toString.stripSuffix(suffix)
    ZIO
      .attemptBlocking {
        Files.createDirectories(archiveDir)
        claimFreeName(archiveDir, stem, suffix)(Files.move(report, _))
      }
      .unit
      .catchAll {
        // A concurrent write to the same directory may have already moved it.
        case _: NoSuchFileException => ZIO.unit
        case e                      => ZIO.logWarning(s"could not archive $report: ${e.safeMessage}")
      }
  }

  /** Makes a file with `create`, at the first of `{stem}{suffix}`, `{stem}.1{suffix}`, `{stem}.2{suffix}`, … that is
    * free; the counter's `.` sorts after the `-` that begins `suffix`, so `{stem}.1` lists after `{stem}`. `create` must
    * throw `FileAlreadyExistsException` rather than replace a file already there.
    */
  private def claimFreeName(dir: Path, stem: String, suffix: String)(create: Path => Path): Path = {
    @tailrec
    def attempt(n: Int): Path = {
      val path = dir.resolve(if (n == 0) s"$stem$suffix" else s"$stem.$n$suffix")
      val taken =
        try { create(path); false }
        catch { case _: FileAlreadyExistsException => true }
      if (taken) attempt(n + 1) else path
    }
    attempt(0)
  }
}
