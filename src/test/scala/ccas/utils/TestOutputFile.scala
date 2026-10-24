package ccas.utils

import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

import scala.jdk.CollectionConverters.*
import scala.util.Using

import com.typesafe.config.ConfigFactory
import zio.ZIO
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.api.misc.subtypes.ClubSlug

/** Reports go under the test config's `output.directory`. Each test clears its own club's directory first, since the
  * reports of earlier runs stay there.
  */
object TestOutputFile extends ZIOSpecDefault {

  private val root: Path = Paths.get(ConfigFactory.load().getString(OutputFile.Root.configKey)).toAbsolutePath

  private def clear(club: ClubSlug): ZIO[Any, Throwable, Path] =
    ZIO.attemptBlocking {
      val dir = root.resolve(ClubSlug.unwrap(club))
      if (Files.exists(dir)) {
        Using.resource(Files.walk(dir))(_.sorted(Comparator.reverseOrder()).iterator().asScala.foreach(Files.delete))
      }
      dir
    }

  override def spec: Spec[Any, Throwable] = suite("OutputFile")(
    test("a report lands under the configured output root, as an absolute path") {
      val club = ClubSlug("output-file-lands")
      for {
        dir  <- clear(club)
        path <- OutputFile.write("history", club, "report body")
        body <- ZIO.attemptBlocking(Files.readString(path))
      } yield assertTrue(path.isAbsolute, path.getParent == dir, body == "report body")
    },
    test("a second report for the same club and app archives the first") {
      val club = ClubSlug("output-file-archives")
      for {
        dir      <- clear(club)
        first    <- OutputFile.write("history", club, "first")
        second   <- OutputFile.write("history", club, "second")
        archived <- ZIO.attemptBlocking(Files.readString(dir.resolve("archive").resolve(first.getFileName)))
        latest   <- ZIO.attemptBlocking(Files.readString(second))
      } yield assertTrue(archived == "first", latest == "second")
    }
  ) @@ TestAspect.sequential
}
