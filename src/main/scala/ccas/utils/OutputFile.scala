package ccas.utils

import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import scala.util.Using

import zio.{Task, ZIO}

import ccas.api.misc.subtypes.ClubSlug

object OutputFile {

  private val dateTimeFormat = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")

  def write(appName: String, clubSlug: ClubSlug, content: String): Task[Path] = {
    val date = LocalDateTime.now().format(dateTimeFormat)
    val dir  = Paths.get("out", ClubSlug.unwrap(clubSlug))
    val path = dir.resolve(s"$date-$appName.txt")
    ZIO.attemptBlocking(Files.createDirectories(dir)) *>
      ZIO.attemptBlocking(archiveExisting(dir, appName)) *>
      ZIO.writeFile(path.toString, content).as(path)
  }

  def writeAndLog(appName: String, clubSlug: ClubSlug, content: String): Task[Unit] =
    write(appName, clubSlug, content).flatMap(path => ZIO.logInfo(s"Output written to $path"))

  def write(appName: String, content: String): Task[Path] = {
    val date = LocalDateTime.now().format(dateTimeFormat)
    val dir  = Paths.get("out")
    val path = dir.resolve(s"$date-$appName.txt")
    ZIO.attemptBlocking(Files.createDirectories(dir)) *>
      ZIO.attemptBlocking(archiveExisting(dir, appName)) *>
      ZIO.writeFile(path.toString, content).as(path)
  }

  def writeAndLog(appName: String, content: String): Task[Unit] =
    write(appName, content).flatMap(path => ZIO.logInfo(s"Output written to $path"))

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
