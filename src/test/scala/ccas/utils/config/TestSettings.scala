package ccas.utils.config

import com.typesafe.config.ConfigFactory
import zio.Config
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

object TestSettings extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("Settings")(
    // The test config hides an exported DATABASE_URL with `url = null`; zio-config alone would read it as "".
    test("a null reads as absent, as Typesafe Config's hasPath has it") {
      val settings = Settings.from(ConfigFactory.parseString("database { url = null, name = ccas_test }"))
      for {
        url  <- settings.read("database.url", Config.string.optional)
        name <- settings.read("database.name", Config.string)
      } yield assertTrue(url.isEmpty, name == "ccas_test")
    },
    test("a dotted path reads the value nested under it") {
      val settings = Settings.from(ConfigFactory.parseString("job-logs { directory = logs }"))
      for {
        directory <- settings.read("job-logs.directory", Config.string.optional)
        absent    <- settings.read("output.directory", Config.string.optional)
      } yield assertTrue(directory.contains("logs"), absent.isEmpty)
    }
  )
}
