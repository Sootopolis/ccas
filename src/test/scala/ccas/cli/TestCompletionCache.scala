package ccas.cli

import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime
import java.time.Instant

import zio.{UIO, ZIO}
import zio.test.{assertTrue, Spec, TestClock, ZIOSpecDefault}

/** Tests [[CompletionCache]] against temp files via its path-explicit `…In` forms — the public no-arg entry points
  * resolve through [[XdgPaths]], whose environment-variable lookups a JVM can't rebind, so driving those would write to
  * the developer's real `~/.cache/ccas`.
  *
  * The TTL boundary runs under `TestClock` (the reason `clubsStaleIn` reads `Clock.instant` rather than
  * `Instant.now()`): mtime is pinned to a fixed epoch and the clock is moved to either side of the 6h window.
  */
object TestCompletionCache extends ZIOSpecDefault {

  private val Base: Instant = Instant.parse("2026-01-01T00:00:00Z")
  private val SixHours: Long = 6L * 60 * 60 * 1000

  // deleteOnExit is LIFO (same reasoning as TestCliConfig): register the dir first, then the leaves it will come to
  // contain, so at JVM exit the leaves are removed before the dir's turn and it is empty by then. Registering the dir
  // alone leaves it populated and the delete silently fails, littering /tmp with a tree per test.
  private def tempDir: UIO[Path] =
    ZIO.attemptBlocking {
      val dir = Files.createTempDirectory("ccas-completion-cache")
      dir.toFile.deleteOnExit()
      dir.resolve("clubs.txt").toFile.deleteOnExit()
      dir.resolve("recent-jobs.txt").toFile.deleteOnExit()
      dir
    }.orDie

  private def clubsFile: UIO[Path] = tempDir.map(_.resolve("clubs.txt"))

  private def setMtime(file: Path, at: Instant): UIO[Unit] =
    ZIO.attemptBlocking(Files.setLastModifiedTime(file, FileTime.from(at))).unit.orDie

  private def read(file: Path): UIO[String] = ZIO.attemptBlocking(Files.readString(file)).orDie

  private def exists(file: Path): UIO[Boolean] = ZIO.attemptBlocking(Files.exists(file)).orDie

  private def mtimeMillis(file: Path): UIO[Long] =
    ZIO.attemptBlocking(Files.getLastModifiedTime(file).toMillis).orDie

  override def spec: Spec[Any, Any] = suite("TestCompletionCache")(
    suite("clubsStale")(
      test("an absent file is stale") {
        for {
          f     <- clubsFile
          stale <- CompletionCache.clubsStaleIn(f)
        } yield assertTrue(stale)
      },
      test("a file written just inside the TTL is fresh") {
        for {
          f <- clubsFile
          _ <- CompletionCache.writeClubsIn(f, List("team-alpha"))
          _ <- setMtime(f, Base)
          _ <- TestClock.setTime(Base.plusMillis(SixHours - 1))
          stale <- CompletionCache.clubsStaleIn(f)
        } yield assertTrue(!stale)
      },
      test("a file older than the TTL is stale") {
        for {
          f <- clubsFile
          _ <- CompletionCache.writeClubsIn(f, List("team-alpha"))
          _ <- setMtime(f, Base)
          _ <- TestClock.setTime(Base.plusMillis(SixHours + 1))
          stale <- CompletionCache.clubsStaleIn(f)
        } yield assertTrue(stale)
      }
    ),
    suite("writeClubs / readClubs")(
      test("round-trips slugs one per line and truncates a longer previous list") {
        for {
          f <- clubsFile
          _ <- CompletionCache.writeClubsIn(f, List("a", "b", "c"))
          _ <- CompletionCache.writeClubsIn(f, List("z"))
          text  <- read(f)
          slugs <- CompletionCache.readClubsIn(f)
        } yield assertTrue(text == "z\n", slugs.contains(List("z")))
      },
      test("an empty list writes an empty file, not a stray newline") {
        for {
          f    <- clubsFile
          _    <- CompletionCache.writeClubsIn(f, Nil)
          text <- read(f)
          slugs <- CompletionCache.readClubsIn(f)
        } yield assertTrue(text.isEmpty, slugs.contains(Nil))
      },
      // Absent is a normal state, not a failure: Some(Nil) = "read it, nothing cached".
      test("readClubs yields Some(Nil) for a missing file") {
        clubsFile.flatMap(CompletionCache.readClubsIn).map(slugs => assertTrue(slugs.contains(Nil)))
      },
      // The distinction the Option exists for: an unreadable cache must NOT masquerade as an empty one. A directory at
      // the cache path forces a genuine read error portably, without chmod games that a root test runner would defeat.
      test("readClubs yields None when the cache exists but cannot be read") {
        for {
          f     <- clubsFile
          _     <- ZIO.attemptBlocking(Files.createDirectories(f)).orDie
          slugs <- CompletionCache.readClubsIn(f)
        } yield assertTrue(slugs.isEmpty)
      },
      test("writeClubs reports true on success") {
        clubsFile.flatMap(CompletionCache.writeClubsIn(_, List("a"))).map(ok => assertTrue(ok))
      },
      // The write signal exists so an unwritable cache can't kill completion silently; same directory trick.
      test("writeClubs reports false when the target cannot be written") {
        for {
          f  <- clubsFile
          _  <- ZIO.attemptBlocking(Files.createDirectories(f)).orDie
          ok <- CompletionCache.writeClubsIn(f, List("a"))
        } yield assertTrue(!ok)
      }
    ),
    suite("seedClubs")(
      test("seeds an absent file and stamps its mtime to the epoch so it still reads as stale") {
        for {
          f <- clubsFile
          _ <- CompletionCache.seedClubsIn(f, List("team-alpha", "team-beta"))
          slugs <- CompletionCache.readClubsIn(f)
          mtime <- mtimeMillis(f)
          _     <- TestClock.setTime(Base)
          stale <- CompletionCache.clubsStaleIn(f)
        } yield assertTrue(slugs.contains(List("team-alpha", "team-beta")), mtime == 0L, stale)
      },
      test("does not overwrite an existing cache") {
        for {
          f <- clubsFile
          _ <- CompletionCache.writeClubsIn(f, List("real"))
          _ <- CompletionCache.seedClubsIn(f, List("seed"))
          slugs <- CompletionCache.readClubsIn(f)
        } yield assertTrue(slugs.contains(List("real")))
      },
      test("an empty seed list creates no file") {
        for {
          f  <- clubsFile
          _  <- CompletionCache.seedClubsIn(f, Nil)
          ex <- exists(f)
        } yield assertTrue(!ex)
      }
    ),
    suite("invalidate")(
      test("deletes the cache so the next staleness check refreshes regardless of the TTL") {
        for {
          f <- clubsFile
          _ <- CompletionCache.writeClubsIn(f, List("team-alpha"))
          _ <- setMtime(f, Base)
          _ <- TestClock.setTime(Base) // well inside the TTL: only the delete can make this stale
          freshBefore <- CompletionCache.clubsStaleIn(f)
          _           <- CompletionCache.invalidateIn(f)
          ex          <- exists(f)
          staleAfter  <- CompletionCache.clubsStaleIn(f)
        } yield assertTrue(!freshBefore, !ex, staleAfter)
      },
      // Pins the load-bearing property: invalidate must DELETE the file (so `clubsStale` refreshes), not blank it —
      // and must not take the containing directory with it.
      test("is a no-op on an absent file, and leaves the cache directory intact") {
        for {
          f      <- clubsFile
          _      <- CompletionCache.invalidateIn(f)
          gone   <- exists(f)
          parent <- exists(f.getParent)
        } yield assertTrue(!gone, parent)
      }
    ),
    suite("appendJob")(
      test("prepends newest-first and drops an earlier duplicate") {
        for {
          dir <- tempDir
          f = dir.resolve("recent-jobs.txt")
          _ <- CompletionCache.appendJobIn(f, "one")
          _ <- CompletionCache.appendJobIn(f, "two")
          _ <- CompletionCache.appendJobIn(f, "one")
          text <- read(f)
        } yield assertTrue(text == "one\ntwo\n")
      },
      test("caps the retained list at 50") {
        for {
          dir <- tempDir
          f = dir.resolve("recent-jobs.txt")
          _    <- ZIO.foreachDiscard(1 to 60)(i => CompletionCache.appendJobIn(f, s"job-$i"))
          text <- read(f)
          lines = text.linesIterator.toList
        } yield assertTrue(lines.size == 50, lines.head == "job-60", lines.last == "job-11")
      }
    )
  )
}
