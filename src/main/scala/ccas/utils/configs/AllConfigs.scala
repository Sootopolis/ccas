package ccas.utils.configs

import ccas.utils.configs
import ccas.utils.prettyprinting.PrettyPrinter
import zio.config.magnolia.DeriveConfig
import zio.{Config, IO}

case class AllConfigs(clubs: Map[String, ClubConfig]) derives PrettyPrinter

object AllConfigs extends CcasConfig[AllConfigs] {
  val root: String = "clubs"
  override val derivedConfig: Config[AllConfigs] = DeriveConfig.derived[AllConfigs].desc

  def load: IO[Config.Error, AllConfigs] = CcasConfig.load(derivedConfig)
}
