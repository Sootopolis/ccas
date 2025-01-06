package ccas.utils.configs

import zio.config.typesafe.TypesafeConfigProvider
import zio.{Config, ConfigProvider, IO}

trait CcasConfig[T <: Product & Serializable] {
  protected val derivedConfig: Config[T] /*= DeriveConfig.derived[T].desc */
}

object CcasConfig {
  private val configProvider: ConfigProvider = TypesafeConfigProvider.fromResourcePath()

  def load[T](config: Config[T]): IO[Config.Error, T] = configProvider.load(config)
}
