package ccas

import java.io.File

import scala.jdk.CollectionConverters.*

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import zio.ZIO
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

// #205 took a relative default off body-store.fs-root and left job-logs.directory's in the same file, for #344 to find
// in production. A path literal resolves against wherever the process starts, so path settings default in code.
object TestShippedConfig extends ZIOSpecDefault {

  // A key whose last word names a path, kebab or camel case: `directory`, `fs-root`, `logDir`, but not `profile`.
  private val PathSetting =
    "(?:.*[-_])?(?:dir|directory|root|path|file|folder)|.*[a-z0-9](?:Dir|Directory|Root|Path|File|Folder)".r

  private val Shipped = new File("src/main/resources/application.conf")

  // `parseFile` reads a missing file as an empty config by default, which would pass this test on nothing.
  private val MustExist = ConfigParseOptions.defaults().setAllowMissing(false)

  override def spec: Spec[Any, Throwable] = suite("ShippedConfig")(
    test("no path setting in the shipped application.conf has a literal default, so none resolves against the launch directory") {
      for {
        resolved <- ZIO.attempt(
          ConfigFactory
            .parseFile(Shipped, MustExist)
            .resolve(ConfigResolveOptions.defaults().setUseSystemEnvironment(false))
        )
        literals = resolved.entrySet.asScala.map(_.getKey).filter(key => PathSetting.matches(key.split('.').last))
      } yield assertTrue(literals.isEmpty)
    }
  )
}
