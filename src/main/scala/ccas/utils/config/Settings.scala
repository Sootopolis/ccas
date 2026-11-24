package ccas.utils.config

import scala.jdk.CollectionConverters.*

import com.typesafe.config.{Config as TypesafeConfig, ConfigFactory, ConfigObject, ConfigUtil, ConfigValueType}
import zio.{Config, IO, Task, TaskLayer, ZIO, ZLayer}
import zio.config.magnolia.DeriveConfig
import zio.config.typesafe.TypesafeConfigProvider

/** This process's settings: `application.conf` resolved against its environment, read once by [[Settings.load]]. Each
  * service reads its own section with [[section]], so a test can hand that service a typed value instead. The server
  * and the CLI's server commands load them after applying the ccas.env file (`CcasServer.withServerEnv`,
  * `Main.serverSettings`); every other entry point reads its process environment alone.
  */
final class Settings private (config: TypesafeConfig) {

  private val provider =
    TypesafeConfigProvider.fromTypesafeConfig(Settings.withoutNulls(config), enableCommaSeparatedValueAsList = true)

  /** The section at the dotted `path`, read as an `A`. */
  def section[A](path: String)(using derived: DeriveConfig[A]): IO[Config.Error, A] = read(path, derived.desc)

  /** The value at the dotted `path`, read by `descriptor`. */
  def read[A](path: String, descriptor: Config[A]): IO[Config.Error, A] =
    provider.load(path.split('.').foldRight(descriptor)((name, nested) => nested.nested(name)))
}

object Settings {

  /** The one place `src/main` reads `application.conf`. */
  val load: Task[Settings] =
    ZIO.attemptBlocking(new Settings(ConfigFactory.load())) // scalafix:ok DisableSyntax.configLoad

  val live: TaskLayer[Settings] = ZLayer(load)

  /** Settings read from `config` alone, for a test that needs a value the test `application.conf` doesn't hold. */
  private[ccas] def from(config: TypesafeConfig): Settings = new Settings(config)

  /** `config` less its null values, so a null reads as absent, as Typesafe Config's own `hasPath` has it, where
    * zio-config would read it as "". The test config hides an exported `DATABASE_URL` with `url = null`.
    */
  private def withoutNulls(config: TypesafeConfig): TypesafeConfig =
    nullPaths(config.root, Vector.empty).foldLeft(config)((remaining, path) => remaining.withoutPath(path))

  private def nullPaths(node: ConfigObject, at: Vector[String]): Vector[String] =
    node.asScala.toVector.flatMap { case (key, value) =>
      value match {
        case nested: ConfigObject                         => nullPaths(nested, at :+ key)
        case _ if value.valueType == ConfigValueType.NULL => Vector(ConfigUtil.joinPath((at :+ key)*))
        case _                                            => Vector.empty
      }
    }
}
