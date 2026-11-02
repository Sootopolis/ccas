package ccas.cli

import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters.*
import scala.util.Using

import zio.{Task, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.info.BuildInfo
import ccas.utils.AppDir.XdgBase

/** Where [[CliWiring]] puts the CLI's files, and what the CLI does when there is nowhere to put them. A JVM can't rebind
  * its environment, so the last two run [[Main]] in a JVM of their own, with no XDG variables and the JDK's `user.home`
  * for a user with no home.
  */
object TestCliWiring extends ZIOSpecDefault {

  private final case class Forked(exitCode: Int, output: String, launchDirEntries: List[Path])

  private def withNoHome(args: String*): Task[Forked] =
    TempWiring.withTempDir { dir =>
      ZIO.attemptBlocking {
        val java    = Paths.get(System.getProperty("java.home"), "bin", "java").toString
        val argv    = List(java, "-Duser.home=?", "-cp", System.getProperty("java.class.path"), "ccas.cli.Main") ++ args
        val builder = new ProcessBuilder(argv*).directory(dir.toFile).redirectErrorStream(true)
        builder.environment().clear()
        val process = builder.start()
        val output  = new String(process.getInputStream.readAllBytes())
        val code    = process.waitFor()
        val entries = Using.resource(Files.list(dir))(_.iterator().asScala.toList)
        Forked(exitCode = code, output = output, launchDirEntries = entries)
      }
    }

  override def spec: Spec[Any, Throwable] = suite("CliWiring")(
    test(
      "each path lands under its own base: config.conf and ccas.env under config, clubs.txt and recent-jobs.txt under cache, pid files under state"
    ) {
      val wiring = CliWiring.resolved(base => Right(Paths.get("/", base.envVar)))
      assertTrue(
        wiring.exists(_.configFile == Paths.get("/XDG_CONFIG_HOME/config.conf")),
        wiring.exists(_.serverEnvFile == Paths.get("/XDG_CONFIG_HOME/ccas.env")),
        wiring.exists(_.clubsFile == Paths.get("/XDG_CACHE_HOME/clubs.txt")),
        wiring.exists(_.recentJobsFile == Paths.get("/XDG_CACHE_HOME/recent-jobs.txt")),
        wiring.exists(_.stateDir == Paths.get("/XDG_STATE_HOME"))
      )
    },
    test("a directory that cannot resolve is a Left saying which directory, and why") {
      val onlyStateMissing = CliWiring.resolved {
        case XdgBase.State => Left("set XDG_STATE_HOME")
        case base          => Right(Paths.get("/", base.envVar))
      }
      assertTrue(onlyStateMissing.left.exists(_ == "cannot find CCAS's state directory: set XDG_STATE_HOME"))
    },
    test(
      "with no home directory, a CLI command exits 2 naming the variable to set and leaves nothing in the launch directory"
    ) {
      // The command that would write `?/.config/ccas/ccas.env` beneath the launch directory if the CLI went ahead.
      for {
        forked <- withNoHome("config", "set", "SERVER_PORT", "9999")
      } yield assertTrue(
        forked.exitCode == 2,
        forked.output.contains("error: cannot find CCAS's config directory"),
        forked.output.contains("Set XDG_CONFIG_HOME to an absolute path."),
        forked.launchDirEntries.isEmpty
      )
    },
    test("--version still answers with no home directory") {
      for {
        forked <- withNoHome("--version")
      } yield assertTrue(forked.exitCode == 0, forked.output.contains(s"ccas ${BuildInfo.version}"))
    }
  )
}
