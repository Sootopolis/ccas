package ccas.utils

import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, ZoneOffset}
import java.util.Comparator

import scala.jdk.CollectionConverters.*
import scala.util.Using

import com.typesafe.config.ConfigFactory
import zio.{LogLevel, UIO, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, TestClock, ZIOSpecDefault, ZTestLogger}

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

  // Each regular file in `dir`, by name, with its contents.
  private def contents(dir: Path): ZIO[Any, Throwable, Map[String, String]] =
    ZIO.attemptBlocking {
      Using.resource(Files.list(dir)) {
        _.iterator().asScala.filter(Files.isRegularFile(_)).map { p =>
          p.getFileName.toString -> Files.readString(p)
        }.toMap
      }
    }

  // Report names are timed to the second, in the system zone; this pins both, so every write below shares a timestamp.
  private val sameSecond: UIO[Unit] =
    TestClock.setTimeZone(ZoneOffset.UTC) *> TestClock.setTime(Instant.parse("2026-10-27T20:33:57Z"))

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
    },
    test("three reports in the same second all survive: the latest in place, the earlier two archived") {
      val club = ClubSlug("output-file-same-second")
      for {
        dir      <- clear(club)
        _        <- sameSecond
        _        <- OutputFile.write("history", club, "first")
        _        <- OutputFile.write("history", club, "second")
        _        <- OutputFile.write("history", club, "third")
        live     <- contents(dir)
        archived <- contents(dir.resolve("archive"))
      } yield assertTrue(
        live == Map("20261027-203357-history.txt" -> "third"),
        archived == Map("20261027-203357-history.txt" -> "first", "20261027-203357.1-history.txt" -> "second")
      )
    },
    test(
      "a report whose archive step fails is still written beside the earlier one, with a warning naming it and why"
    ) {
      val club = ClubSlug("output-file-archive-fails")
      for {
        dir   <- clear(club)
        _     <- sameSecond
        first <- OutputFile.write("history", club, "first")
        // A file where `archive/` belongs, so the archive step can't create it.
        _        <- ZIO.attemptBlocking(Files.createFile(dir.resolve("archive")))
        _        <- OutputFile.write("history", club, "second")
        live     <- contents(dir)
        warnings <- ZTestLogger.logOutput.map(_.filter(_.logLevel == LogLevel.Warning).map(_.message()))
      } yield assertTrue(
        live == Map(
          "archive"                       -> "",
          "20261027-203357-history.txt"   -> "first",
          "20261027-203357.1-history.txt" -> "second"
        ),
        warnings.size == 1,
        warnings.exists(w => w.contains(first.toString) && w.contains(s"${dir.resolve("archive")}: File exists"))
      )
    }.provide(ZTestLogger.default)
  ) @@ TestAspect.sequential
}
