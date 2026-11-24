package ccas.cli

import java.nio.file.Files

import com.typesafe.config.ConfigFactory
import zio.{Config, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** [[Main.serverSettings]], in a suite of its own: the test sets a JVM system property and drops Typesafe Config's
  * cache, which a sibling test loading config in parallel could race (see `TestServerEnvOverlay`).
  */
object TestServerSettings extends ZIOSpecDefault {

  private val Key = "CCAS_TEST_SERVER_SETTINGS"

  override def spec: Spec[Any, Throwable] = suite("Main.serverSettings")(
    // A system property is a config path in its own right, so the settings read the key itself.
    test("the settings are loaded after ccas.env is applied, so a value set only in the file reaches them") {
      (for {
        dir <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-server-settings"))
        file = dir.resolve("ccas.env")
        _ <- ZIO.attemptBlocking {
          dir.toFile.deleteOnExit()
          Files.writeString(file, s"$Key=from-the-file\n").toFile.deleteOnExit()
        }
        settings <- Main.serverSettings(file)
        got      <- settings.read(Key, Config.string)
      } yield assertTrue(got == "from-the-file")).ensuring(ZIO.succeed {
        System.clearProperty(Key)
        ConfigFactory.invalidateCaches()
      })
    }
  )
}
