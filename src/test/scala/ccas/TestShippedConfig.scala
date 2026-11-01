package ccas

import java.io.File

import scala.jdk.CollectionConverters.*

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions, ConfigValueType}
import zio.ZIO
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.server.config.ServerEnvKey.Values.Toggle
import ccas.server.config.ServerEnvKeys

// #205 took a relative default off body-store.fs-root and left job-logs.directory's in the same file, for #344 to find
// in production. A path literal resolves against wherever the process starts, so path settings default in code.
object TestShippedConfig extends ZIOSpecDefault {

  // A key whose last word names a path, kebab or camel case: `directory`, `fs-root`, `logDir`, but not `profile`.
  private val PathSetting =
    "(?:.*[-_])?(?:dir|directory|root|path|file|folder)|.*[a-z0-9](?:Dir|Directory|Root|Path|File|Folder)".r

  private val Shipped = new File("src/main/resources/application.conf")

  // `parseFile` reads a missing file as an empty config by default, which would pass these tests on nothing.
  private val MustExist = ConfigParseOptions.defaults().setAllowMissing(false)

  private val NoEnvironment = ConfigResolveOptions.defaults().setUseSystemEnvironment(false)

  private def isBoolean(config: Config, path: String): Boolean =
    config.hasPath(path) && config.getValue(path).valueType == ConfigValueType.BOOLEAN

  override def spec: Spec[Any, Throwable] = suite("ShippedConfig")(
    test(
      "no path setting in the shipped application.conf has a literal default, so none resolves against the launch directory"
    ) {
      for {
        resolved <- ZIO.attempt(ConfigFactory.parseFile(Shipped, MustExist).resolve(NoEnvironment))
        literals = resolved.entrySet.asScala.map(_.getKey).filter(key => PathSetting.matches(key.split('.').last))
      } yield assertTrue(literals.isEmpty)
    },
    // `ccas config set` offers true|false for a toggle and warns on what getBoolean won't read, so a boolean setting
    // whose key isn't a toggle goes without both; #203's own list of them missed DB_TCP_KEEP_ALIVE.
    test("the toggles in ServerEnvKeys are exactly the env keys whose shipped default is a boolean") {
      // Every registry key set to its own name, so a resolved value says which key reached that path. Layered over the
      // file rather than passed to `resolveWith`, which can't resolve a `x = 6` / `x = ${?VAR}` pair from a separate
      // source ("replace in parent not possible").
      val ownNames = ConfigFactory.parseMap(ServerEnvKeys.all.map(key => key.name -> key.name).toMap.asJava)
      for {
        parsed     <- ZIO.attempt(ConfigFactory.parseFile(Shipped, MustExist))
        defaults   <- ZIO.attempt(parsed.resolve(NoEnvironment))
        overridden <- ZIO.attempt(ownNames.withFallback(parsed).resolve(NoEnvironment))
        reached     = overridden.entrySet.asScala.toList.filter(entry => isBoolean(defaults, entry.getKey))
        booleanKeys = reached.map(_.getValue.unwrapped).collect { case name: String => name }.toSet
        toggles     = ServerEnvKeys.all.filter(_.values == Toggle).map(_.name).toSet
      } yield assertTrue(booleanKeys.nonEmpty, booleanKeys == toggles)
    }
  )
}
