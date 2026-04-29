package ccas.utils

import java.nio.file.{Files, NoSuchFileException, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import scala.util.Using

import zio.{Task, ZIO}

import ccas.api.misc.subtypes.ClubSlug

object OutputFile {

  private val dateTimeFormat = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")

  def write(appName: String, clubSlug: ClubSlug, content: String, ext: String = "txt"): Task[Path] =
    writeInternal(Paths.get("out", ClubSlug.unwrap(clubSlug)), appName, content, ext)

  def writeAndLog(appName: String, clubSlug: ClubSlug, content: String, ext: String = "txt"): Task[Unit] =
    write(appName, clubSlug, content, ext).flatMap(path => ZIO.logInfo(s"Output written to $path"))

  def writeGlobal(appName: String, content: String, subDir: String, ext: String = "txt"): Task[Path] =
    writeInternal(Paths.get("out", subDir), appName, content, ext)

  private def writeInternal(dir: Path, appName: String, content: String, ext: String): Task[Path] = {
    val date = LocalDateTime.now().format(dateTimeFormat)
    val path = dir.resolve(s"$date-$appName.$ext")
    ZIO.attemptBlocking(Files.createDirectories(dir)) *>
      ZIO.attemptBlocking(archiveExisting(dir, appName, ext)) *>
      ZIO.writeFile(path.toString, content).as(path)
  }

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
