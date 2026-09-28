package ccas.scripts

import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions

import scala.jdk.CollectionConverters.*

import zio.{Task, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.server.config.ServerEnvFile

/** Runs `scripts/backup-neon.sh` against a stand-in `pg_dump` that records its arguments and `PGPASSWORD`. The
  * environment is cleared and `HOME` is a temp dir, so neither a developer's exported `DATABASE_URL` nor their real
  * `ccas.env` can reach the script, and every run reads and writes the default XDG paths under it.
  */
object TestBackupNeonScript extends ZIOSpecDefault {

  private final case class Run(exitCode: Int, output: String, argv: List[String], password: String, home: Path) {
    def conninfo: String = argv.headOption.getOrElse("")
  }

  // One argument per line, then the `--file` target the script renames into place.
  private val FakePgDump =
    """#!/bin/sh
      |printf '%s\n' "$@" > "$CCAS_TEST_ROOT/argv"
      |printf '%s' "${PGPASSWORD-}" > "$CCAS_TEST_ROOT/pgpassword"
      |for arg in "$@"; do
      |  case "$arg" in --file=*) : > "${arg#--file=}" ;; esac
      |done
      |""".stripMargin

  private def run(env: Map[String, String], envFile: Option[String]): Task[Run] =
    for {
      root <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-backup-neon"))
      _    <- ZIO.attemptBlocking(installFakePgDump(root))
      _    <- ZIO.foreachDiscard(envFile)(content => ZIO.attemptBlocking(writeEnvFile(root, content)))
      proc <- ZIO.attemptBlocking(start(root, env))
      out  <- ZIO.attemptBlocking(new String(proc.getInputStream.readAllBytes()))
      code <- ZIO.attemptBlocking(proc.waitFor())
      argv <- readIfPresent(root.resolve("argv"))
      pass <- readIfPresent(root.resolve("pgpassword"))
    } yield Run(exitCode = code, output = out, argv = argv.linesIterator.toList, password = pass, home = root)

  private def installFakePgDump(root: Path): Unit = {
    val bin = Files.createDirectories(root.resolve("bin"))
    Files.writeString(bin.resolve("pg_dump"), FakePgDump)
    bin.resolve("pg_dump").toFile.setExecutable(true)
    ()
  }

  private def writeEnvFile(root: Path, content: String): Unit = {
    val file = root.resolve(".config/ccas/ccas.env")
    Files.createDirectories(file.getParent)
    Files.writeString(file, content)
    ()
  }

  // `/bin/bash`, not `env bash`: on macOS that is 3.2, the bash cron runs the script with.
  private def start(root: Path, env: Map[String, String]): Process = {
    val builder = new ProcessBuilder("/bin/bash", "scripts/backup-neon.sh").redirectErrorStream(true)
    val vars    = builder.environment()
    vars.clear()
    vars.putAll(
      (Map(
        "PATH"           -> s"${root.resolve("bin")}:/usr/bin:/bin",
        "HOME"           -> root.toString,
        "CCAS_TEST_ROOT" -> root.toString
      ) ++ env).asJava
    )
    builder.start()
  }

  private def readIfPresent(file: Path): Task[String] =
    ZIO.attemptBlocking(Option.when(Files.exists(file))(Files.readString(file)).getOrElse(""))

  private def permissions(path: Path): Task[String] =
    ZIO.attemptBlocking(PosixFilePermissions.toString(Files.getPosixFilePermissions(path)))

  override def spec: Spec[Any, Any] = suite("TestBackupNeonScript")(
    test("a JDBC URL's pgjdbc parameters reach pg_dump under their libpq names, the password in PGPASSWORD") {
      val url =
        "jdbc:postgresql://db.example:5432/ccas?user=owner&password=s3cret&sslmode=require&channelBinding=require"
      for {
        result <- run(env = Map("DATABASE_URL" -> url), envFile = None)
      } yield assertTrue(
        result.exitCode == 0,
        result.conninfo == "postgresql://db.example:5432/ccas?user=owner&sslmode=require&channel_binding=require",
        result.password == "s3cret",
        !result.argv.exists(_.contains("s3cret"))
      )
    },
    test("pgjdbc parameters with no libpq equivalent are dropped and counted, never named") {
      val url =
        "jdbc:postgresql://h/ccas?connectTimeout=10&socketTimeout=30&tcpKeepAlive=true&ssl=false&prepareThreshold=0"
      for {
        result <- run(env = Map("DATABASE_URL" -> url), envFile = None)
      } yield assertTrue(
        result.exitCode == 0,
        result.conninfo == "postgresql://h/ccas?connect_timeout=10",
        result.output.contains("dropped 4 URL parameter(s) pg_dump would reject"),
        !result.output.contains("socketTimeout")
      )
    },
    test("a password holding an unescaped & or ? stops the backup before pg_dump, echoing no piece of it") {
      val urls = List(
        "jdbc:postgresql://h/ccas?user=u&password=frag1&frag2&sslmode=require",
        "postgresql://owner:frag3?frag4@h/ccas",
        "postgresql://owner:frag5?x=frag6@h/ccas"
      )
      for {
        results <- ZIO.foreach(urls)(url => run(env = Map("DATABASE_URL" -> url), envFile = None))
      } yield assertTrue(results.forall(r => r.exitCode != 0 && r.argv.isEmpty && !r.output.contains("frag")))
    },
    test("ssl=true goes first, so an explicit sslmode still wins over it as it does in pgjdbc") {
      val url = "jdbc:postgresql://h/ccas?sslmode=verify-full&ssl=true"
      for {
        result <- run(env = Map("DATABASE_URL" -> url), envFile = None)
      } yield assertTrue(result.exitCode == 0, result.conninfo == "postgresql://h/ccas?ssl=true&sslmode=verify-full")
    },
    test("a URL ending in a bare ? dumps without a query") {
      for {
        result <- run(env = Map("DATABASE_URL" -> "jdbc:postgresql://h/ccas?"), envFile = None)
      } yield assertTrue(result.exitCode == 0, result.conninfo == "postgresql://h/ccas")
    },
    test("a libpq URI keeps its own parameters and gives up its userinfo password") {
      val url = "postgresql://owner:p%40ss+1@h/ccas?sslmode=require&channel_binding=require"
      for {
        result <- run(env = Map("DATABASE_URL" -> url), envFile = None)
      } yield assertTrue(
        result.exitCode == 0,
        result.conninfo == "postgresql://owner@h/ccas?sslmode=require&channel_binding=require",
        result.password == "p@ss+1",
        !result.output.contains("dropped")
      )
    },
    test("the output names the host and database dumped, and no credential") {
      val url = "postgresql://owner:s3cret@db.example:5432/ccas?sslmode=require"
      for {
        result <- run(env = Map("DATABASE_URL" -> url), envFile = None)
        wrote   = result.output.linesIterator.find(_.startsWith("wrote ")).getOrElse("")
      } yield assertTrue(
        result.exitCode == 0,
        wrote.endsWith(" from db.example:5432/ccas"),
        !result.output.contains("owner"),
        !result.output.contains("s3cret")
      )
    },
    test("ccas.env supplies DATABASE_URL when the environment has none") {
      val file =
        """# written by ccas config
          |export DATABASE_URL=jdbc:postgresql://file-host/ccas?user=owner&password=file-password&channelBinding=require
          |""".stripMargin
      for {
        result <- run(env = Map.empty, envFile = Some(file))
      } yield assertTrue(
        result.exitCode == 0,
        result.conninfo == "postgresql://file-host/ccas?user=owner&channel_binding=require",
        result.password == "file-password"
      )
    },
    test("the environment's DATABASE_URL beats ccas.env's") {
      for {
        result <- run(
          env = Map("DATABASE_URL" -> "jdbc:postgresql://env-host/ccas"),
          envFile = Some("DATABASE_URL=jdbc:postgresql://file-host/ccas\n")
        )
      } yield assertTrue(result.exitCode == 0, result.conninfo == "postgresql://env-host/ccas")
    },
    test("ccas.env's DATABASE_URL beats the environment's DB_* fields, as it does for the server") {
      val dbFields =
        Map("DB_HOST" -> "env-host", "DB_NAME" -> "ccas", "DB_USER" -> "owner", "DB_PASSWORD" -> "env-password")
      for {
        result <- run(env = dbFields, envFile = Some("DATABASE_URL=jdbc:postgresql://file-host/ccas\n"))
      } yield assertTrue(result.exitCode == 0, result.conninfo == "postgresql://file-host/ccas")
    },
    test("ccas.env values are read as ServerEnvFile reads them, a blank environment value deferring to the file") {
      val file =
        """DB_HOST=stale-host
          |DB_HOST = file-host
          |DB_NAME='ccas'
          |DB_USER=owner
          |DB_PASSWORD="a \"quoted\" #pass\\word"
          |""".stripMargin
      val server = ServerEnvFile.toMap(ServerEnvFile.parseLines(file))
      for {
        result <- run(env = Map("DB_HOST" -> " "), envFile = Some(file))
      } yield assertTrue(
        result.exitCode == 0,
        result.conninfo == s"postgresql://${server("DB_HOST")}:5432/${server("DB_NAME")}" +
          s"?user=${server("DB_USER")}&sslmode=require",
        result.password == server("DB_PASSWORD")
      )
    },
    test("dumps land owner-only under ~/.local/share/ccas/backups by default") {
      for {
        result   <- run(env = Map("DATABASE_URL" -> "jdbc:postgresql://h/ccas"), envFile = None)
        dir       = result.home.resolve(".local/share/ccas/backups")
        dumps    <- ZIO.attemptBlocking(dir.toFile.listFiles().toList.map(_.toPath))
        dirPerms <- permissions(dir)
        perms    <- ZIO.foreach(dumps)(permissions)
      } yield assertTrue(result.exitCode == 0, dirPerms == "rwx------", perms == List("rw-------"))
    }
  ) @@ TestAspect.sequential
}
