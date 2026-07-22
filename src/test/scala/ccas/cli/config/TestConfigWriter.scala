package ccas.cli.config

import java.nio.file.{Files, Path}

import zio.{UIO, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Tests [[ConfigWriter]] against temp config files.
  *
  * Set side: creates the file (and parents) when absent, preserves other keys and comments, overwrites an existing
  * `current_club` rather than duplicating it. Clear side: removes the key while leaving other content untouched, never
  * creates a config that was never written, and leaves a key that can be set again without duplicating. Every case that
  * leaves a file behind round-trips through [[CliConfig.load]] to confirm it still parses. No server, no DB.
  */
object TestConfigWriter extends ZIOSpecDefault {

  private def tempDir: UIO[Path] =
    ZIO.attemptBlocking {
      val dir = Files.createTempDirectory("ccas-config-writer")
      dir.toFile.deleteOnExit()
      dir
    }.orDie

  private def read(p: Path): UIO[String] = ZIO.attemptBlocking(Files.readString(p)).orDie

  private def load(file: Path): UIO[CliConfig] = CliConfig.load(file).orDieWith(s => new RuntimeException(s))

  override def spec: Spec[Any, Any] = suite("TestConfigWriter")(
    test("creates the file and parent dirs when absent") {
      for {
        dir <- tempDir
        file = dir.resolve("nested/config.conf")
        _   <- ConfigWriter.setCurrentClub(file, "team-alpha").orDie
        cfg <- load(file)
      } yield assertTrue(cfg.currentClub.contains("team-alpha"))
    },
    test("preserves other keys and comments while setting current_club") {
      val existing =
        """# my ccas config
          |api_url = "http://host:9000"
          |default_clubs = ["a", "b"]
          |""".stripMargin
      for {
        dir <- tempDir
        file = dir.resolve("config.conf")
        _    <- ZIO.attemptBlocking(Files.writeString(file, existing)).orDie
        _    <- ConfigWriter.setCurrentClub(file, "team-beta").orDie
        text <- read(file)
        cfg  <- load(file)
      } yield assertTrue(
        text.contains("# my ccas config"),
        text.contains("api_url = \"http://host:9000\""),
        cfg.apiUrl.contains("http://host:9000"),
        cfg.defaultClubs == List("a", "b"),
        cfg.currentClub.contains("team-beta")
      )
    },
    test("overwrites an existing current_club rather than duplicating it") {
      for {
        dir <- tempDir
        file = dir.resolve("config.conf")
        _    <- ConfigWriter.setCurrentClub(file, "first").orDie
        _    <- ConfigWriter.setCurrentClub(file, "second").orDie
        text <- read(file)
        cfg  <- load(file)
      } yield assertTrue(
        cfg.currentClub.contains("second"),
        text.linesIterator.count(_.trim.startsWith("current_club")) == 1
      )
    },
    test("clearCurrentClub removes the key while preserving other keys and comments") {
      val existing =
        """# my ccas config
          |api_url = "http://host:9000"
          |current_club = "team-alpha"
          |""".stripMargin
      for {
        dir <- tempDir
        file = dir.resolve("config.conf")
        _    <- ZIO.attemptBlocking(Files.writeString(file, existing)).orDie
        _    <- ConfigWriter.clearCurrentClub(file).orDie
        text <- read(file)
        cfg  <- load(file)
      } yield assertTrue(
        cfg.currentClub.isEmpty,
        cfg.apiUrl.contains("http://host:9000"),
        text.contains("# my ccas config"),
        !text.contains("current_club")
      )
    },
    test("clearCurrentClub on a config holding only that key leaves an empty file that still parses") {
      for {
        dir <- tempDir
        file = dir.resolve("config.conf")
        _    <- ConfigWriter.setCurrentClub(file, "team-alpha").orDie
        _    <- ConfigWriter.clearCurrentClub(file).orDie
        text <- read(file)
        cfg  <- load(file)
      } yield assertTrue(text.isEmpty, cfg.currentClub.isEmpty)
    },
    test("clearCurrentClub does not create a config that was never written") {
      for {
        dir <- tempDir
        file = dir.resolve("config.conf")
        _      <- ConfigWriter.clearCurrentClub(file).orDie
        exists <- ZIO.attemptBlocking(Files.exists(file)).orDie
      } yield assertTrue(!exists)
    },
    test("a cleared key can be set again") {
      for {
        dir <- tempDir
        file = dir.resolve("config.conf")
        _    <- ConfigWriter.setCurrentClub(file, "first").orDie
        _    <- ConfigWriter.clearCurrentClub(file).orDie
        _    <- ConfigWriter.setCurrentClub(file, "second").orDie
        cfg  <- load(file)
        text <- read(file)
      } yield assertTrue(
        cfg.currentClub.contains("second"),
        text.linesIterator.count(_.trim.startsWith("current_club")) == 1
      )
    }
  )
}
