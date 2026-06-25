package ccas.server.config

import java.nio.file.{Files, Path}

import com.typesafe.config.ConfigFactory
import zio.{UIO, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Tests [[ServerEnvOverlay]]: a file value is promoted to a JVM system property only when the env var and property are
  * both unset, and Typesafe Config then resolves both optional `${?VAR}` and required `${VAR}` substitutions to it. The
  * env-wins case is exercised via a pre-set system property (a stand-in: a process env var can't be set in-JVM, but the
  * code guards on env OR property and skips either way). Each case uses a private `CCAS_TEST_OVERLAY_*` key and clears it
  * in `ensuring` so nothing leaks across suites (the repo also runs `Test / parallelExecution := false`). No app.conf
  * dependency: the substitution resolution is proven self-contained with `defaultOverrides` + `parseString`.
  */
object TestServerEnvOverlay extends ZIOSpecDefault {

  private def tempDir: UIO[Path] =
    ZIO.attemptBlocking {
      val dir = Files.createTempDirectory("ccas-overlay")
      dir.toFile.deleteOnExit()
      dir
    }.orDie

  private def write(p: Path, content: String): UIO[Unit] = ZIO.attemptBlocking(Files.writeString(p, content)).unit.orDie

  // Run `zio`, always clearing the given properties (and dropping the systemProperties cache) afterwards.
  private def withCleanProps[A](keys: String*)(zio: UIO[A]): UIO[A] =
    zio.ensuring(ZIO.succeed {
      keys.foreach(System.clearProperty)
      ConfigFactory.invalidateCaches()
    })

  // Read the property INTO a value (via ZIO.succeed) so the assertion references a stable binding. ZIO Test's
  // smart-assertion re-evaluates a bare `System.getProperty(...)` expression during result rendering — which happens
  // after `withCleanProps`'s `ensuring` clears it — so reading it directly in `assertTrue` would always see None.
  private def prop(key: String): UIO[Option[String]] = ZIO.succeed(Option(System.getProperty(key)))

  override def spec: Spec[Any, Any] = suite("TestServerEnvOverlay")(
    test("applies a file value as a system property when env and property are unset") {
      withCleanProps("CCAS_TEST_OVERLAY_A") {
        for {
          dir <- tempDir
          file = dir.resolve("ccas.env")
          _       <- write(file, "CCAS_TEST_OVERLAY_A=hello\n")
          applied <- ServerEnvOverlay(file)
          got     <- prop("CCAS_TEST_OVERLAY_A")
        } yield assertTrue(applied.contains("CCAS_TEST_OVERLAY_A"), got.contains("hello"))
      }
    },
    test("ConfigFactory resolves an optional ${?VAR} substitution to the overlaid value") {
      withCleanProps("CCAS_TEST_OVERLAY_B") {
        for {
          dir <- tempDir
          file = dir.resolve("ccas.env")
          _ <- write(file, "CCAS_TEST_OVERLAY_B=42\n")
          _ <- ServerEnvOverlay(file)
          resolved <- ZIO.attempt {
            ConfigFactory
              .defaultOverrides()
              .withFallback(ConfigFactory.parseString("v = ${?CCAS_TEST_OVERLAY_B}"))
              .resolve()
              .getString("v")
          }.orDie
        } yield assertTrue(resolved == "42")
      }
    },
    test("ConfigFactory resolves a required ${VAR} substitution to the overlaid value") {
      withCleanProps("CCAS_TEST_OVERLAY_C") {
        for {
          dir <- tempDir
          file = dir.resolve("ccas.env")
          _ <- write(file, "CCAS_TEST_OVERLAY_C=req\n")
          _ <- ServerEnvOverlay(file)
          resolved <- ZIO.attempt {
            ConfigFactory
              .defaultOverrides()
              .withFallback(ConfigFactory.parseString("v = ${CCAS_TEST_OVERLAY_C}"))
              .resolve()
              .getString("v")
          }.orDie
        } yield assertTrue(resolved == "req")
      }
    },
    test("does not overwrite an already-set property (env-wins stand-in)") {
      withCleanProps("CCAS_TEST_OVERLAY_D") {
        for {
          dir <- tempDir
          file = dir.resolve("ccas.env")
          _       <- ZIO.succeed(System.setProperty("CCAS_TEST_OVERLAY_D", "preset"))
          _       <- write(file, "CCAS_TEST_OVERLAY_D=fromfile\n")
          applied <- ServerEnvOverlay(file)
          got     <- prop("CCAS_TEST_OVERLAY_D")
        } yield assertTrue(!applied.contains("CCAS_TEST_OVERLAY_D"), got.contains("preset"))
      }
    },
    test("a blank value is not applied (the HOCON default stands)") {
      withCleanProps("CCAS_TEST_OVERLAY_E") {
        for {
          dir <- tempDir
          file = dir.resolve("ccas.env")
          _       <- write(file, "CCAS_TEST_OVERLAY_E=\n")
          applied <- ServerEnvOverlay(file)
          got     <- prop("CCAS_TEST_OVERLAY_E")
        } yield assertTrue(applied.isEmpty, got.isEmpty)
      }
    },
    test("is idempotent: a second run applies nothing new") {
      withCleanProps("CCAS_TEST_OVERLAY_F") {
        for {
          dir <- tempDir
          file = dir.resolve("ccas.env")
          _  <- write(file, "CCAS_TEST_OVERLAY_F=once\n")
          a1 <- ServerEnvOverlay(file)
          a2 <- ServerEnvOverlay(file)
        } yield assertTrue(a1.contains("CCAS_TEST_OVERLAY_F"), a2.isEmpty)
      }
    },
    test("a missing file applies nothing and does not fail") {
      for {
        dir     <- tempDir
        applied <- ServerEnvOverlay(dir.resolve("does-not-exist.env"))
      } yield assertTrue(applied.isEmpty)
    }
  )
}
