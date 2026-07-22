package ccas.cli

import java.nio.file.{Files, NoSuchFileException, Path}
import java.nio.file.attribute.FileTime

import scala.jdk.CollectionConverters.*

import zio.{Clock, UIO, ZIO}
import zio.json.{DeriveJsonDecoder, JsonDecoder}

/** Maintains the cache files the generated shell completions read — club slugs and recent job ids. Every operation is
  * best-effort: IO errors are swallowed and never change a command's exit code (completion is a convenience, not
  * correctness). Files live under [[XdgPaths.cacheDir]], matching the paths the emitted scripts read.
  *
  * Each operation has a package-private `…In` variant taking the target file explicitly. [[XdgPaths]] resolves its
  * directories from environment variables, which a JVM can't rebind at runtime, so the public no-arg entry points would
  * otherwise only be exercisable against the developer's real `~/.cache/ccas` — the parameterised forms are what the
  * tests drive.
  */
object CompletionCache {

  // Refresh the clubs cache only when missing or older than this, so a /api/managed-clubs round-trip isn't added to
  // every command — just an occasional one.
  private val ClubsTtlMillis: Long = 6L * 60 * 60 * 1000

  // Cap on retained recent job ids (newest first).
  private val MaxRecentJobs = 50

  /** Minimal CLI-local mirror of the server's package-private `ClubsResponse` — only the slug is needed downstream. */
  private[cli] final case class ClubDto(slug: String, name: String)
  private[cli] object ClubDto {
    given JsonDecoder[ClubDto] = DeriveJsonDecoder.gen
  }

  private[cli] final case class ClubsDto(clubs: List[ClubDto])
  private[cli] object ClubsDto {
    given JsonDecoder[ClubsDto] = DeriveJsonDecoder.gen
  }

  /** True when the clubs cache is absent or older than the TTL (any IO error is treated as "stale" → refresh). */
  def clubsStale: UIO[Boolean] = clubsStaleIn(XdgPaths.clubsFile)

  /** Read the cached club slugs (one per line). Used by `ccas use-club` for an offline "unknown slug" hint — never a
    * source of truth. See [[readClubsIn]] for why this is an `Option` rather than a bare list.
    */
  def readClubs: UIO[Option[List[String]]] = readClubsIn(XdgPaths.clubsFile)

  /** Overwrite the clubs cache with one slug per line (the endpoint already sorts them). `false` means the write failed
    * and the cache is unchanged, so completion won't reflect this list — an unwritable cache directory otherwise leaves
    * completion permanently dead with nothing said. Callers for whom the refresh is incidental should discard the
    * result; a caller acting on an explicit request about club setup should report it. Never fails.
    */
  def writeClubs(slugs: List[String]): UIO[Boolean] = writeClubsIn(XdgPaths.clubsFile, slugs)

  /** Seed the clubs cache from the config's `default_clubs` so completion has suggestions before any server round-trip.
    */
  def seedClubs(clubs: List[String]): UIO[Unit] = seedClubsIn(XdgPaths.clubsFile, clubs)

  /** Delete the clubs cache so the next [[clubsStale]] check refreshes it, bypassing the TTL. Called when the managed
    * set changes under us (`club add`/`remove`) and when a submit fails with "Club not found" — the cached slug the
    * user just tab-completed is very likely the stale one, and waiting out the 6h TTL would re-suggest it on retry.
    */
  def invalidate: UIO[Unit] = invalidateIn(XdgPaths.clubsFile)

  /** Prepend a job id (newest first), dropping any earlier duplicate and capping the list at [[MaxRecentJobs]]. */
  def appendJob(jobId: String): UIO[Unit] = appendJobIn(XdgPaths.recentJobsFile, jobId)

  // --- Path-explicit forms (see the object scaladoc) ---

  /** Reads the time via `Clock.instant` rather than `Instant.now()` so the TTL boundary is testable under `TestClock`.
    */
  private[cli] def clubsStaleIn(file: Path): UIO[Boolean] =
    Clock.instant.flatMap(now =>
      ZIO.attemptBlocking {
        !Files.exists(file) || {
          val ageMillis = now.toEpochMilli - Files.getLastModifiedTime(file).toInstant.toEpochMilli
          ageMillis > ClubsTtlMillis
        }
      }.orElseSucceed(true)
    )

  /** `Some(slugs)` when the cache was read — an empty list then genuinely means "no clubs cached". `None` when the file
    * is there but unreadable (bad permissions, corrupt bytes, not a regular file).
    *
    * The distinction matters: "I know of no clubs" and "I could not find out" are different answers, and collapsing
    * them to `Nil` makes the second silently indistinguishable from the first — so the *least* certain state produces
    * the *least* output. An unreadable cache is also a persistent, user-fixable condition that breaks shell completion
    * too (the generated scripts `cat` this same file), so callers should say something rather than shrug. Still never
    * fails: a broken cache must not change any command's exit code. A file that vanishes mid-read counts as absent, not
    * unreadable — that is a benign race with `invalidate`, not a misconfiguration.
    */
  private[cli] def readClubsIn(file: Path): UIO[Option[List[String]]] =
    ZIO
      .attemptBlocking {
        if (Files.exists(file)) { Files.readAllLines(file).asScala.toList.map(_.trim).filter(_.nonEmpty) }
        else { Nil }
      }
      .map(Some(_))
      .catchSome { case _: NoSuchFileException => ZIO.some(Nil) }
      .orElseSucceed(None)

  private[cli] def writeClubsIn(file: Path, slugs: List[String]): UIO[Boolean] =
    ZIO.attemptBlocking {
      createParent(file)
      val trailing = if (slugs.isEmpty) { "" } else { "\n" }
      Files.writeString(file, slugs.mkString("", "\n", trailing))
    }.isSuccess

  /** No-op when the list is empty or the cache already exists (an authoritative refresh must win). The seed file's
    * mtime is stamped to the epoch so [[clubsStaleIn]] still treats it as stale — the next server-touching command
    * replaces it with real slugs rather than trusting the seed for the full TTL.
    */
  private[cli] def seedClubsIn(file: Path, clubs: List[String]): UIO[Unit] =
    ZIO.attemptBlocking {
      if (clubs.nonEmpty && !Files.exists(file)) {
        createParent(file)
        Files.writeString(file, clubs.mkString("", "\n", "\n"))
        Files.setLastModifiedTime(file, FileTime.fromMillis(0L))
        ()
      }
    }.ignore

  private[cli] def invalidateIn(file: Path): UIO[Unit] =
    ZIO.attemptBlocking(Files.deleteIfExists(file)).unit.ignore

  private[cli] def appendJobIn(file: Path, jobId: String): UIO[Unit] =
    ZIO.attemptBlocking {
      createParent(file)
      val existing =
        if (Files.exists(file)) { Files.readAllLines(file).asScala.toList.filter(_.nonEmpty) } else { Nil }
      val updated = (jobId :: existing.filterNot(_ == jobId)).take(MaxRecentJobs)
      Files.writeString(file, updated.mkString("", "\n", "\n"))
    }.ignore

  private def createParent(file: Path): Unit = {
    Option(file.getParent).foreach(Files.createDirectories(_))
    ()
  }
}
