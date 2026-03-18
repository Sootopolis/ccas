package ccas.utils

import java.nio.file.{Files, Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import zio.{Task, ZIO}

import ccas.api.misc.subtypes.ClubUrlName

object OutputFile {

  private val dateTimeFormat = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")

  def write(appName: String, clubUrlName: ClubUrlName, content: String): Task[Path] = {
    val date = LocalDateTime.now().format(dateTimeFormat)
    val dir = Paths.get("out", appName)
    val path = dir.resolve(s"$date-$clubUrlName.txt")
    ZIO.attemptBlocking(Files.createDirectories(dir)) *>
      ZIO.writeFile(path.toString, content).as(path)
  }
}
