package ccas.cli

import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.time.Instant

import scala.jdk.CollectionConverters.*

import zio.{UIO, ZIO}
import zio.json.{DeriveJsonDecoder, JsonDecoder}

/** Maintains the cache files the generated shell completions read — club slugs and recent job ids. Every operation is
  * best-effort: IO errors are swallowed and never change a command's exit code (completion is a convenience, not
  * correctness). Files live under [[XdgPaths.cacheDir]], matching the paths the emitted scripts read.
  */
object CompletionCache {

  // Refresh the clubs cache only when missing or older than this, so a /api/clubs round-trip isn't added to every
  // command — just an occasional one.
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
  def clubsStale: UIO[Boolean] =
    ZIO.attemptBlocking {
      val f = XdgPaths.clubsFile
      !Files.exists(f) || {
        val ageMillis = Instant.now().toEpochMilli - Files.getLastModifiedTime(f).toInstant.toEpochMilli
        ageMillis > ClubsTtlMillis
      }
    }.orElseSucceed(true)

  /** Overwrite the clubs cache with one slug per line (the endpoint already sorts them). */
  def writeClubs(slugs: List[String]): UIO[Unit] =
    ZIO.attemptBlocking {
      Files.createDirectories(XdgPaths.cacheDir)
      Files.writeString(XdgPaths.clubsFile, slugs.mkString("", "\n", if (slugs.isEmpty) "" else "\n"))
    }.ignore

  /** Seed the clubs cache from the config's `default_clubs` so completion has suggestions before any server round-trip.
    * No-op when the list is empty or the cache already exists (an authoritative `/api/clubs` refresh must win). The
    * seed file's mtime is stamped to the epoch so [[clubsStale]] still treats it as stale — the next server-touching
    * command replaces it with real slugs rather than trusting the seed for the full TTL.
    */
  def seedClubs(clubs: List[String]): UIO[Unit] =
    ZIO.attemptBlocking {
      val f = XdgPaths.clubsFile
      if (clubs.nonEmpty && !Files.exists(f)) {
        Files.createDirectories(XdgPaths.cacheDir)
        Files.writeString(f, clubs.mkString("", "\n", "\n"))
        Files.setLastModifiedTime(f, FileTime.fromMillis(0L))
        ()
      }
    }.ignore

  /** Prepend a job id (newest first), dropping any earlier duplicate and capping the list at [[MaxRecentJobs]]. */
  def appendJob(jobId: String): UIO[Unit] =
    ZIO.attemptBlocking {
      val f = XdgPaths.recentJobsFile
      Files.createDirectories(XdgPaths.cacheDir)
      val existing = if (Files.exists(f)) Files.readAllLines(f).asScala.toList.filter(_.nonEmpty) else Nil
      val updated = (jobId :: existing.filterNot(_ == jobId)).take(MaxRecentJobs)
      Files.writeString(f, updated.mkString("", "\n", "\n"))
    }.ignore
}
