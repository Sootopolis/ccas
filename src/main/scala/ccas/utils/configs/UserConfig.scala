package ccas.utils.configs

import ccas.api.misc.subtypes.Username
import ccas.ccasVersion
import ccas.utils.configs
import zio.Config
import zio.config.magnolia.DeriveConfig
import zio.http.Header.UserAgent
import zio.http.Header.UserAgent.ProductOrComment.{Comment, Product}
import zio.http.{Header, Headers}

case class UserConfig(username: Username, email: String) {
  // TODO: Make this configurable
  val headers: Headers = {
    val comments = List(
      Comment("Repository: https://www.github.com/Sootopolis/ccas/"),
      Comment("Developer email: wallace.dev@proton.me"),
      Comment(s"User chess.com username: $username"),
      Comment(s"User email: $email"),
    )
    Headers(UserAgent(Product("CCAS", Some(ccasVersion)), comments))
  }
}

object UserConfig extends CcasConfig[UserConfig] {
  override protected val derivedConfig: Config[UserConfig] = DeriveConfig.derived[configs.UserConfig].desc
}
