package ccas.utils.configs

import ccas.utils.client.CcasClient
import ccas.utils.configs
import ccas.utils.prettyprinting.PrettyPrinter
import zio.config.magnolia.DeriveConfig
import zio.http.Client
import zio.{Config, IO, RIO}

case class AllConfigs(user: UserConfig, clubs: Map[String, BaseClubConfig]) derives PrettyPrinter {
  def client: RIO[Client, CcasClient] = CcasClient.create(user.headers)
}

object AllConfigs extends CcasConfig[AllConfigs] {
  val root: String = "clubs"
  override val derivedConfig: Config[AllConfigs] = DeriveConfig.derived[AllConfigs].desc

  def load: IO[Config.Error, AllConfigs] = CcasConfig.load(derivedConfig)
}
