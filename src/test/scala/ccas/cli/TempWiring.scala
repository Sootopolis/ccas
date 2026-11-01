package ccas.cli

import java.nio.file.{Files, Path}

import zio.{UIO, ZIO}
import zio.http.{Response, Routes}

/** A [[CliWiring]] whose files sit in a fresh temp directory and whose client answers from a [[FakeCcasServer]], so a
  * command run by a test stores what it stores there, never in the developer's own config, cache and `ccas.env`.
  */
object TempWiring {

  def apply(routes: Routes[Any, Response]): UIO[CliWiring] = tempDir.map(in(_, routes))

  def in(dir: Path, routes: Routes[Any, Response]): CliWiring =
    CliWiring(
      configFile = dir.resolve(ConfigFile),
      clubsFile = dir.resolve(ClubsFile),
      recentJobsFile = dir.resolve(RecentJobsFile),
      serverEnvFile = dir.resolve(ServerEnvFile),
      clientLayer = FakeCcasServer.layer(routes)
    )

  // deleteOnExit runs last-registered first, so the leaves go before the directory, which is empty by its turn.
  def tempDir: UIO[Path] =
    ZIO.attemptBlocking {
      val dir = Files.createTempDirectory("ccas-cli")
      dir.toFile.deleteOnExit()
      List(ConfigFile, ClubsFile, RecentJobsFile, ServerEnvFile).foreach { leaf =>
        dir.resolve(leaf).toFile.deleteOnExit()
      }
      dir
    }.orDie

  /** A regular file in `dir` where a test needs a directory, so that a write beneath it cannot land. */
  def blocker(dir: Path): UIO[Path] =
    ZIO.attemptBlocking {
      val file = Files.writeString(dir.resolve("not-a-dir"), "")
      file.toFile.deleteOnExit()
      file
    }.orDie

  private val ConfigFile     = "config.conf"
  private val ClubsFile      = "clubs.txt"
  private val RecentJobsFile = "recent-jobs.txt"
  private val ServerEnvFile  = "ccas.env"
}
