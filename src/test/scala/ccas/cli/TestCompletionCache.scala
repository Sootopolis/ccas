package ccas.cli

import java.io.{EOFException, IOException}
import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime
import java.time.Instant

import zio.{Console, IO, Trace, UIO, ZIO}
import zio.test.{assertTrue, Spec, TestClock, TestConsole, ZIOSpecDefault}

/** Tests [[CompletionCache]] against temp files. The TTL boundary runs under `TestClock` (the reason `clubsStale`
  * reads `Clock.instant` rather than `Instant.now()`): mtime is pinned to a fixed epoch and the clock is moved to either
  * side of the 6h window.
  */
object TestCompletionCache extends ZIOSpecDefault {

  private val Base: Instant  = Instant.parse("2026-01-01T00:00:00Z")
  private val SixHours: Long = 6L * 60 * 60 * 1000

  private def clubsFile: UIO[Path] = TempWiring.tempDir.map(_.resolve("clubs.txt"))

  private def setMtime(file: Path, at: Instant): UIO[Unit] =
    ZIO.attemptBlocking(Files.setLastModifiedTime(file, FileTime.from(at))).unit.orDie

  private def read(file: Path): UIO[String] = ZIO.attemptBlocking(Files.readString(file)).orDie

  // A console whose stderr is gone, as under `ccas … 2>&-`.
  private val closedStderr: Console = new Console {
    private def closed(using Trace): IO[IOException, Unit]           = ZIO.fail(new IOException("Bad file descriptor"))
    def print(line: => Any)(using Trace): IO[IOException, Unit]      = ZIO.unit
    def printError(line: => Any)(using Trace): IO[IOException, Unit] = closed
    def printLine(line: => Any)(using Trace): IO[IOException, Unit]  = ZIO.unit
    def printLineError(line: => Any)(using Trace): IO[IOException, Unit] = closed
    def readLine(using Trace): IO[IOException, String]                   = ZIO.fail(new EOFException)
  }

  private def exists(file: Path): UIO[Boolean] = ZIO.attemptBlocking(Files.exists(file)).orDie

  private def mtimeMillis(file: Path): UIO[Long] =
    ZIO.attemptBlocking(Files.getLastModifiedTime(file).toMillis).orDie

  override def spec: Spec[Any, Any] = suite("TestCompletionCache")(
    suite("clubsStale")(
      test("an absent file is stale") {
        for {
          f     <- clubsFile
          stale <- CompletionCache.clubsStale(f)
        } yield assertTrue(stale)
      },
      test("a file written just inside the TTL is fresh") {
        for {
          f     <- clubsFile
          _     <- CompletionCache.writeClubs(f, List("team-alpha"))
          _     <- setMtime(f, Base)
          _     <- TestClock.setTime(Base.plusMillis(SixHours - 1))
          stale <- CompletionCache.clubsStale(f)
        } yield assertTrue(!stale)
      },
      test("a file older than the TTL is stale") {
        for {
          f     <- clubsFile
          _     <- CompletionCache.writeClubs(f, List("team-alpha"))
          _     <- setMtime(f, Base)
          _     <- TestClock.setTime(Base.plusMillis(SixHours + 1))
          stale <- CompletionCache.clubsStale(f)
        } yield assertTrue(stale)
      }
    ),
    suite("writeClubs / readClubs")(
      test("round-trips slugs one per line and truncates a longer previous list") {
        for {
          f     <- clubsFile
          _     <- CompletionCache.writeClubs(f, List("a", "b", "c"))
          _     <- CompletionCache.writeClubs(f, List("z"))
          text  <- read(f)
          slugs <- CompletionCache.readClubs(f)
        } yield assertTrue(text == "z\n", slugs.contains(List("z")))
      },
      // Two clubs can answer to one name since #254, and the same name offered twice helps nobody at the prompt.
      test("a slug two clubs answer to is offered once") {
        for {
          f     <- clubsFile
          _     <- CompletionCache.writeClubs(f, List("team-alpha", "team-beta", "team-alpha"))
          text  <- read(f)
          slugs <- CompletionCache.readClubs(f)
        } yield assertTrue(text == "team-alpha\nteam-beta\n", slugs.contains(List("team-alpha", "team-beta")))
      },
      test("an empty list writes an empty file, not a stray newline") {
        for {
          f     <- clubsFile
          _     <- CompletionCache.writeClubs(f, Nil)
          text  <- read(f)
          slugs <- CompletionCache.readClubs(f)
        } yield assertTrue(text.isEmpty, slugs.contains(Nil))
      },
      // Absent is a normal state, not a failure: Some(Nil) = "read it, nothing cached".
      test("readClubs yields Some(Nil) for a missing file") {
        clubsFile.flatMap(CompletionCache.readClubs).map(slugs => assertTrue(slugs.contains(Nil)))
      },
      // The distinction the Option exists for: an unreadable cache must NOT masquerade as an empty one. A directory at
      // the cache path forces a genuine read error portably, without chmod games that a root test runner would defeat.
      test("readClubs yields None when the cache exists but cannot be read") {
        for {
          f     <- clubsFile
          _     <- ZIO.attemptBlocking(Files.createDirectories(f)).orDie
          slugs <- CompletionCache.readClubs(f)
        } yield assertTrue(slugs.isEmpty)
      },
      test("writeClubs reports true on success") {
        clubsFile.flatMap(CompletionCache.writeClubs(_, List("a"))).map(ok => assertTrue(ok))
      },
      // The write signal exists so an unwritable cache can't kill completion silently; same directory trick.
      test("writeClubs reports false when the target cannot be written") {
        for {
          f  <- clubsFile
          _  <- ZIO.attemptBlocking(Files.createDirectories(f)).orDie
          ok <- CompletionCache.writeClubs(f, List("a"))
        } yield assertTrue(!ok)
      }
    ),
    suite("reportClubsWriteFailed")(
      test("names the cache file in a warning on stderr") {
        for {
          file   <- clubsFile
          _      <- CompletionCache.reportClubsWriteFailed(file)
          stderr <- TestConsole.outputErr
        } yield assertTrue(stderr.mkString.contains(s"could not update the club cache at $file"))
      },
      test("never fails, even when stderr can't be written") {
        for {
          file <- clubsFile
          exit <- ZIO.withConsole(closedStderr)(CompletionCache.reportClubsWriteFailed(file)).exit
        } yield assertTrue(exit.isSuccess)
      }
    ),
    suite("seedClubs")(
      test("seeds an absent file and stamps its mtime to the epoch so it still reads as stale") {
        for {
          f     <- clubsFile
          _     <- CompletionCache.seedClubs(f, List("team-alpha", "team-beta"))
          slugs <- CompletionCache.readClubs(f)
          mtime <- mtimeMillis(f)
          _     <- TestClock.setTime(Base)
          stale <- CompletionCache.clubsStale(f)
        } yield assertTrue(slugs.contains(List("team-alpha", "team-beta")), mtime == 0L, stale)
      },
      test("does not overwrite an existing cache") {
        for {
          f     <- clubsFile
          _     <- CompletionCache.writeClubs(f, List("real"))
          _     <- CompletionCache.seedClubs(f, List("seed"))
          slugs <- CompletionCache.readClubs(f)
        } yield assertTrue(slugs.contains(List("real")))
      },
      test("an empty seed list creates no file") {
        for {
          f  <- clubsFile
          _  <- CompletionCache.seedClubs(f, Nil)
          ex <- exists(f)
        } yield assertTrue(!ex)
      }
    ),
    suite("invalidate")(
      test("deletes the cache so the next staleness check refreshes regardless of the TTL") {
        for {
          f           <- clubsFile
          _           <- CompletionCache.writeClubs(f, List("team-alpha"))
          _           <- setMtime(f, Base)
          _           <- TestClock.setTime(Base) // well inside the TTL: only the delete can make this stale
          freshBefore <- CompletionCache.clubsStale(f)
          _           <- CompletionCache.invalidate(f)
          ex          <- exists(f)
          staleAfter  <- CompletionCache.clubsStale(f)
        } yield assertTrue(!freshBefore, !ex, staleAfter)
      },
      // Pins the load-bearing property: invalidate must DELETE the file (so `clubsStale` refreshes), not blank it —
      // and must not take the containing directory with it.
      test("is a no-op on an absent file, and leaves the cache directory intact") {
        for {
          f      <- clubsFile
          _      <- CompletionCache.invalidate(f)
          gone   <- exists(f)
          parent <- exists(f.getParent)
        } yield assertTrue(!gone, parent)
      }
    ),
    suite("appendJob")(
      test("prepends newest-first and drops an earlier duplicate") {
        for {
          dir <- TempWiring.tempDir
          f = dir.resolve("recent-jobs.txt")
          _    <- CompletionCache.appendJob(f, "one")
          _    <- CompletionCache.appendJob(f, "two")
          _    <- CompletionCache.appendJob(f, "one")
          text <- read(f)
        } yield assertTrue(text == "one\ntwo\n")
      },
      test("caps the retained list at 50") {
        for {
          dir <- TempWiring.tempDir
          f = dir.resolve("recent-jobs.txt")
          _    <- ZIO.foreachDiscard(1 to 60)(i => CompletionCache.appendJob(f, s"job-$i"))
          text <- read(f)
          lines = text.linesIterator.toList
        } yield assertTrue(lines.size == 50, lines.head == "job-60", lines.last == "job-11")
      }
    )
  )
}
