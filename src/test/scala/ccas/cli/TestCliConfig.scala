package ccas.cli

import java.nio.file.{Files, Path}

import zio.{UIO, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.cli.config.CliConfig

/** Tests [[CliConfig.load]] against temp HOCON files: missing file → defaults, full/partial parse, `~` expansion, and a
  * malformed file failing with a clean, file-naming message. No server, no DB.
  */
object TestCliConfig extends ZIOSpecDefault {

  private val home = System.getProperty("user.home")

  // deleteOnExit is LIFO, so the dir is registered before the file it contains — at JVM exit the file is removed first,
  // then the now-empty dir — leaving no temp litter from test runs.
  private def tempDir: UIO[Path] =
    ZIO.attemptBlocking {
      val dir = Files.createTempDirectory("ccas-cli-config")
      dir.toFile.deleteOnExit()
      dir
    }.orDie

  private def writeConfig(content: String): UIO[Path] =
    tempDir.flatMap(dir =>
      ZIO.attemptBlocking {
        val f = dir.resolve("config.conf")
        Files.writeString(f, content)
        f.toFile.deleteOnExit()
        f
      }.orDie
    )

  override def spec: Spec[Any, Any] = suite("TestCliConfig")(
    test("missing file resolves to the empty config") {
      tempDir.flatMap(dir => CliConfig.load(dir.resolve("config.conf"))).map(cfg => assertTrue(cfg == CliConfig.empty))
    },
    test("full config parses every key and expands ~ in log_dir") {
      val content =
        """api_url = "http://host:9000"
          |default_clubs = ["team-alpha", "team-beta"]
          |log_dir = "~/.local/state/ccas/logs"
          |current_club = "team-alpha"
          |""".stripMargin
      writeConfig(content).flatMap(CliConfig.load).map { cfg =>
        assertTrue(
          cfg.apiUrl.contains("http://host:9000"),
          cfg.defaultClubs == List("team-alpha", "team-beta"),
          cfg.logDir.contains(s"$home/.local/state/ccas/logs"),
          cfg.currentClub.contains("team-alpha")
        )
      }
    },
    test("partial config defaults the absent keys") {
      writeConfig("api_url = \"http://only:1\"\n").flatMap(CliConfig.load).map { cfg =>
        assertTrue(
          cfg.apiUrl.contains("http://only:1"),
          cfg.defaultClubs.isEmpty,
          cfg.logDir.isEmpty,
          cfg.currentClub.isEmpty
        )
      }
    },
    test("blank current_club is treated as unset") {
      writeConfig("current_club = \"  \"\n").flatMap(CliConfig.load).map(cfg => assertTrue(cfg.currentClub.isEmpty))
    },
    test("malformed config fails with a message naming the file") {
      writeConfig("api_url = \"unterminated\n").flatMap { f =>
        CliConfig.load(f).either.map(res =>
          assertTrue(res.left.exists(m => m.contains("invalid config file") && m.contains(f.toString)))
        )
      }
    }
  )
}
