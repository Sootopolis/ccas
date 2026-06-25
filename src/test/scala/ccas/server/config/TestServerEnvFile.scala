package ccas.server.config

import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermission

import scala.jdk.CollectionConverters.*

import zio.{UIO, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Tests [[ServerEnvFile]] against temp files: round-trip, comment/order preservation, JDBC-URL safety (split on first
  * `=`), quote handling, in-place replace, idempotent unset, malformed/duplicate-line tolerance, and owner-only (0600)
  * perms on the written file. No server, no DB.
  */
object TestServerEnvFile extends ZIOSpecDefault {

  private def tempDir: UIO[Path] =
    ZIO.attemptBlocking {
      val dir = Files.createTempDirectory("ccas-server-env")
      dir.toFile.deleteOnExit()
      dir
    }.orDie

  private def write(p: Path, content: String): UIO[Unit] = ZIO.attemptBlocking(Files.writeString(p, content)).unit.orDie

  private def read(p: Path): UIO[String] = ZIO.attemptBlocking(Files.readString(p)).orDie

  override def spec: Spec[Any, Any] = suite("TestServerEnvFile")(
    test("set then readMap round-trips multiple keys") {
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _ <- ServerEnvFile.set(file, "CCAS_CONTACT_EMAIL", "me@x.com").orDie
        _ <- ServerEnvFile.set(file, "SERVER_PORT", "9090").orDie
        m <- ServerEnvFile.readMap(file).orDie
      } yield assertTrue(m.get("CCAS_CONTACT_EMAIL").contains("me@x.com"), m.get("SERVER_PORT").contains("9090"))
    },
    test("preserves comments, blanks, and ordering when setting a new key") {
      val existing =
        """# header comment
          |CCAS_CONTACT_EMAIL=me@x.com
          |
          |# db section
          |DB_HOST=localhost
          |""".stripMargin
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _    <- write(file, existing)
        _    <- ServerEnvFile.set(file, "SERVER_PORT", "8080").orDie
        text <- read(file)
        m    <- ServerEnvFile.readMap(file).orDie
        lines = text.linesIterator.toList
      } yield assertTrue(
        text.contains("# header comment"),
        text.contains("# db section"),
        lines.indexWhere(_.startsWith("DB_HOST")) < lines.indexWhere(_.startsWith("SERVER_PORT")),
        m.get("DB_HOST").contains("localhost"),
        m.get("SERVER_PORT").contains("8080")
      )
    },
    test("a JDBC URL with & ? = : survives set/read unchanged") {
      val url = "jdbc:postgresql://h:5432/db?user=x&password=y&sslmode=require"
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _   <- ServerEnvFile.set(file, "DATABASE_URL", url).orDie
        got <- ServerEnvFile.get(file, "DATABASE_URL").orDie
      } yield assertTrue(got.contains(url))
    },
    test("a value with spaces round-trips via quoting") {
      val v = "SELECT 1"
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _    <- ServerEnvFile.set(file, "DB_POOL_CONNECTION_TEST_QUERY", v).orDie
        text <- read(file)
        got  <- ServerEnvFile.get(file, "DB_POOL_CONNECTION_TEST_QUERY").orDie
      } yield assertTrue(got.contains(v), text.contains("\"SELECT 1\""))
    },
    test("a single-quoted value parses literally") {
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _   <- write(file, "DB_USER='joe'\n")
        got <- ServerEnvFile.get(file, "DB_USER").orDie
      } yield assertTrue(got.contains("joe"))
    },
    test("a leading 'export ' is stripped from the key") {
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _ <- write(file, "export DB_HOST=db.example\n")
        m <- ServerEnvFile.readMap(file).orDie
      } yield assertTrue(m.get("DB_HOST").contains("db.example"), !m.contains("export DB_HOST"))
    },
    test("setting an existing key replaces it in place without duplicating") {
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _    <- ServerEnvFile.set(file, "SERVER_PORT", "8080").orDie
        _    <- ServerEnvFile.set(file, "SERVER_PORT", "9090").orDie
        text <- read(file)
        m    <- ServerEnvFile.readMap(file).orDie
      } yield assertTrue(m.get("SERVER_PORT").contains("9090"), text.linesIterator.count(_.startsWith("SERVER_PORT")) == 1)
    },
    test("unset removes a present key (true) and is a no-op for an absent one (false)") {
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _  <- ServerEnvFile.set(file, "SERVER_PORT", "8080").orDie
        r1 <- ServerEnvFile.unset(file, "SERVER_PORT").orDie
        r2 <- ServerEnvFile.unset(file, "SERVER_PORT").orDie
        m  <- ServerEnvFile.readMap(file).orDie
      } yield assertTrue(r1, !r2, !m.contains("SERVER_PORT"))
    },
    test("a malformed line without = is preserved and does not break parsing") {
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _    <- write(file, "garbage no equals\nSERVER_PORT=8080\n")
        _    <- ServerEnvFile.set(file, "SERVER_HOST", "0.0.0.0").orDie
        text <- read(file)
        m    <- ServerEnvFile.readMap(file).orDie
      } yield assertTrue(
        text.contains("garbage no equals"),
        m.get("SERVER_PORT").contains("8080"),
        m.get("SERVER_HOST").contains("0.0.0.0")
      )
    },
    test("duplicate keys resolve last-wins in readMap") {
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _ <- write(file, "SERVER_PORT=1\nSERVER_PORT=2\n")
        m <- ServerEnvFile.readMap(file).orDie
      } yield assertTrue(m.get("SERVER_PORT").contains("2"))
    },
    test("the written file is owner-only (0600)") {
      for {
        dir <- tempDir
        file = dir.resolve("ccas.env")
        _     <- ServerEnvFile.set(file, "SERVER_PORT", "8080").orDie
        perms <- ZIO.attemptBlocking(Files.getPosixFilePermissions(file).asScala.toSet).orDie
      } yield assertTrue(perms == Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE))
    }
  )
}
