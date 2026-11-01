package ccas.cli

import java.nio.file.{Files, Path}

import zio.{ExitCode, UIO, ZIO}
import zio.http.Routes
import zio.test.{assertTrue, Spec, TestAspect, TestConsole, ZIOSpecDefault}

import ccas.server.config.ServerEnvFile

/** Tests [[ConfigCommand]]'s handlers whole, each against a `ccas.env` in its own [[TempWiring]] directory, so what
  * they print, the exit code they return and what they leave in the file are pinned without reaching the developer's
  * own. The file model itself is [[ccas.server.config.TestServerEnvFile]]'s.
  */
object TestConfigCommand extends ZIOSpecDefault {

  private def envFile: UIO[Path] = TempWiring(Routes.empty).map(_.serverEnvFile)

  private def seeded(content: String): UIO[Path] =
    envFile.tap(file => ZIO.attemptBlocking(Files.writeString(file, content)).orDie)

  private def read(file: Path): UIO[String] = ZIO.attemptBlocking(Files.readString(file)).orDie

  private def exists(file: Path): UIO[Boolean] = ZIO.attemptBlocking(Files.exists(file)).orDie

  /** What a command printed to stdout, as lines, however many prints it took. */
  private def outLines: UIO[List[String]] = TestConsole.output.map(_.mkString.linesIterator.toList)

  private val url = "jdbc:postgresql://db.example/ccas?user=admin&password=hunter2"

  // Contact email, the JDBC-URL branch, the URL, Enter for the default port, Enter to save.
  private val wizardAnswers = List("me@example.com", "1", url, "", "")

  override def spec: Spec[Any, Any] = suite("TestConfigCommand")(
    suite("get")(
      test("prints a set key's value, a secret's in full") {
        for {
          file <- seeded("DB_PASSWORD=hunter2\n")
          code <- ConfigCommand.get(file)("DB_PASSWORD")
          out  <- TestConsole.output
        } yield assertTrue(code == ExitCode.success, out == Vector("hunter2\n"))
      },
      test("a key that isn't set exits 2 and names it") {
        for {
          file <- envFile
          code <- ConfigCommand.get(file)("SERVER_PORT")
          err  <- TestConsole.outputErr
        } yield assertTrue(code == ExitCode(2), err == Vector("error: SERVER_PORT is not set\n"))
      }
    ),
    test("a blank key exits 2 from get, set and unset, and writes nothing") {
      for {
        file <- envFile
        codes <- ZIO.collectAll(
          List(ConfigCommand.get(file)("  "), ConfigCommand.set(file)("  ", "x"), ConfigCommand.unset(file)("  "))
        )
        err   <- TestConsole.outputErr
        wrote <- exists(file)
      } yield assertTrue(
        codes == List.fill(3)(ExitCode(2)),
        err == Vector.fill(3)("error: key must not be blank\n"),
        !wrote
      )
    },
    suite("set")(
      test("stores the value and echoes a secret masked") {
        for {
          file   <- envFile
          code   <- ConfigCommand.set(file)("DB_PASSWORD", "hunter2")
          out    <- TestConsole.output
          stored <- ServerEnvFile.get(file, "DB_PASSWORD")
        } yield assertTrue(
          code == ExitCode.success,
          out == Vector("set DB_PASSWORD = ****\n"),
          stored.contains("hunter2")
        )
      },
      test("a blank value exits 2, points at unset, and writes nothing") {
        for {
          file  <- envFile
          code  <- ConfigCommand.set(file)("SERVER_PORT", "   ")
          err   <- TestConsole.outputErr
          wrote <- exists(file)
        } yield assertTrue(
          code == ExitCode(2),
          err == Vector(
            "error: value for SERVER_PORT must not be blank (use 'ccas config unset SERVER_PORT' to remove it)\n"
          ),
          !wrote
        )
      },
      test("a key ccas doesn't know is written, with a warning") {
        for {
          file   <- envFile
          code   <- ConfigCommand.set(file)("MY_SETTING", "x")
          err    <- TestConsole.outputErr
          stored <- ServerEnvFile.get(file, "MY_SETTING")
        } yield assertTrue(
          code == ExitCode.success,
          err == Vector("warning: 'MY_SETTING' is not a known ccas setting (writing anyway)\n"),
          stored.contains("x")
        )
      },
      test("an unusable DATABASE_URL is written, with a warning that never echoes it") {
        val unusable = "mysql://admin:hunter2@db.example/ccas"
        for {
          file   <- envFile
          code   <- ConfigCommand.set(file)("DATABASE_URL", unusable)
          out    <- TestConsole.output
          err    <- TestConsole.outputErr
          stored <- ServerEnvFile.get(file, "DATABASE_URL")
        } yield assertTrue(
          code == ExitCode.success,
          err.size == 1,
          err.head.startsWith("warning: database.url has an unrecognised scheme"),
          err.head.endsWith("(writing anyway)\n"),
          !(out ++ err).exists(_.contains("hunter2")),
          stored.contains(unusable)
        )
      },
      test("a JDBC or libpq DATABASE_URL raises no warning") {
        for {
          file  <- envFile
          jdbc  <- ConfigCommand.set(file)("DATABASE_URL", url)
          libpq <- ConfigCommand.set(file)("DATABASE_URL", "postgresql://admin:hunter2@db.example/ccas")
          err   <- TestConsole.outputErr
        } yield assertTrue(jdbc == ExitCode.success, libpq == ExitCode.success, err.isEmpty)
      },
      test("a file that can't be written exits 1") {
        for {
          dir     <- TempWiring.tempDir
          blocker <- TempWiring.blocker(dir)
          code    <- ConfigCommand.set(TempWiring.in(blocker, Routes.empty).serverEnvFile)("SERVER_PORT", "9090")
          err     <- TestConsole.outputErr
        } yield assertTrue(code == ExitCode(1), err.size == 1, err.head.startsWith("error: "))
      }
    ),
    suite("unset")(
      test("removes a set key, and says when it wasn't set") {
        for {
          file   <- seeded("SERVER_PORT=9090\n")
          first  <- ConfigCommand.unset(file)("SERVER_PORT")
          second <- ConfigCommand.unset(file)("SERVER_PORT")
          out    <- TestConsole.output
          stored <- ServerEnvFile.get(file, "SERVER_PORT")
        } yield assertTrue(
          first == ExitCode.success,
          second == ExitCode.success,
          out == Vector("unset SERVER_PORT\n", "SERVER_PORT was not set\n"),
          stored.isEmpty
        )
      }
    ),
    suite("list")(
      test("masks secrets unless asked to show them") {
        for {
          file   <- seeded("DB_PASSWORD=hunter2\nCCAS_R2_SECRET_KEY=s3cr3t\nSERVER_PORT=9090\n")
          _      <- ConfigCommand.list(file)(showSecrets = false)
          masked <- outLines
          _      <- TestConsole.clearOutput
          _      <- ConfigCommand.list(file)(showSecrets = true)
          shown  <- outLines
        } yield assertTrue(
          masked.contains("DB_PASSWORD=****"),
          masked.contains("CCAS_R2_SECRET_KEY=****"),
          masked.contains("SERVER_PORT=9090"),
          !masked.exists(line => line.contains("hunter2") || line.contains("s3cr3t")),
          shown.contains("DB_PASSWORD=hunter2"),
          shown.contains("CCAS_R2_SECRET_KEY=s3cr3t")
        )
      },
      test("shows a known key that isn't set as (unset), and keys it doesn't know under their own header") {
        for {
          file <- seeded("MY_SETTING=x\n")
          code <- ConfigCommand.list(file)(showSecrets = false)
          out  <- outLines
        } yield assertTrue(
          code == ExitCode.success,
          out.contains("SERVER_PORT=(unset)"),
          out.takeRight(2) == List("# Other keys set in the file:", "MY_SETTING=x")
        )
      }
    ),
    test("path prints the file it manages") {
      for {
        file <- envFile
        code <- ConfigCommand.path(file)
        out  <- TestConsole.output
      } yield assertTrue(code == ExitCode.success, out == Vector(s"$file\n"))
    },
    suite("init")(
      test("with no file, writes the answers and masks secrets in the preview") {
        for {
          file   <- envFile
          _      <- TestConsole.feedLines(wizardAnswers*)
          code   <- ConfigCommand.init(file)
          out    <- TestConsole.output
          stored <- ServerEnvFile.readMap(file)
        } yield assertTrue(
          code == ExitCode.success,
          stored == Map("CCAS_CONTACT_EMAIL" -> "me@example.com", "DATABASE_URL" -> url),
          out.contains("  DATABASE_URL=****\n"),
          !out.exists(_.contains("hunter2")),
          out.last == s"wrote $file\n"
        )
      },
      test("over an existing file, answering no leaves it untouched") {
        val before = "# mine\nSERVER_PORT=9090\n"
        for {
          file  <- seeded(before)
          _     <- TestConsole.feedLines("n")
          code  <- ConfigCommand.init(file)
          out   <- TestConsole.output
          after <- read(file)
        } yield assertTrue(code == ExitCode.success, out.last == "aborted\n", after == before)
      },
      test("over an existing file, answering yes updates what it asked and keeps the rest") {
        for {
          file   <- seeded("CCAS_CONTACT_EMAIL=old@example.com\nSERVER_PORT=9090\n")
          _      <- TestConsole.feedLines(("y" :: wizardAnswers)*)
          code   <- ConfigCommand.init(file)
          stored <- ServerEnvFile.readMap(file)
        } yield assertTrue(
          code == ExitCode.success,
          stored == Map("CCAS_CONTACT_EMAIL" -> "me@example.com", "DATABASE_URL" -> url, "SERVER_PORT" -> "9090")
        )
      },
      test("without a terminal, exits 1 and says it needs one") {
        for {
          file  <- envFile
          code  <- ConfigCommand.init(file)
          err   <- TestConsole.outputErr
          wrote <- exists(file)
        } yield assertTrue(
          code == ExitCode(1),
          err == Vector("error: 'ccas config init' requires an interactive terminal\n"),
          !wrote
        )
      }
    )
  ) @@ TestAspect.silent // `list` prints every known key; three of those would bury the rest of the suite's log.
}
