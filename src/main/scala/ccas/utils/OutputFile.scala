package ccas.utils

import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import scala.util.Using

import zio.{RIO, Task, ZIO}

import ccas.api.misc.subtypes.ClubSlug

object OutputFile {

  private val dateTimeFormat = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")

  def write(appName: String, clubSlug: ClubSlug, content: String): Task[Path] =
    writeInternal(Paths.get("out", ClubSlug.unwrap(clubSlug)), appName, content)

  def writeAndLog(appName: String, clubSlug: ClubSlug, content: String): RIO[CcasLogger, Unit] =
    write(appName, clubSlug, content).flatMap(path => CcasLogger.info(s"Output written to $path"))

  def writeGlobal(appName: String, content: String, subDir: String): Task[Path] =
    writeInternal(Paths.get("out", subDir), appName, content)

  private def writeInternal(dir: Path, appName: String, content: String): Task[Path] = {
    val date = LocalDateTime.now().format(dateTimeFormat)
    val path = dir.resolve(s"$date-$appName.txt")
    ZIO.attemptBlocking(Files.createDirectories(dir)) *>
      ZIO.attemptBlocking(archiveExisting(dir, appName)) *>
      ZIO.writeFile(path.toString, content).as(path)
  }

  def writeAndLogGlobal(appName: String, content: String, subDir: String): RIO[CcasLogger, Unit] =
    writeGlobal(appName, content, subDir).flatMap(path => CcasLogger.info(s"Output written to $path"))

  private def archiveExisting(clubDir: Path, appName: String): Unit =
    if (Files.exists(clubDir)) {
      val suffix     = s"-$appName.txt"
      val archiveDir = clubDir.resolve("archive")
      Using(Files.list(clubDir)) { stream =>
        stream
          .filter(p => Files.isRegularFile(p) && p.getFileName.toString.endsWith(suffix))
          .forEach { p =>
            Files.createDirectories(archiveDir)
            Files.move(p, archiveDir.resolve(p.getFileName)): Unit
          }
      }: Unit
    }
}
