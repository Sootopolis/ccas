package ccas.server.config

import zio.{RIO, ZIO}
import zio.config.magnolia.DeriveConfig

import ccas.utils.config.Settings

/** The `server` section of `application.conf`: where the HTTP server listens. */
final case class ServerConfig(port: Int, host: String)

object ServerConfig {

  given DeriveConfig[ServerConfig] = DeriveConfig.derived

  val load: RIO[Settings, ServerConfig] = ZIO.serviceWithZIO[Settings](_.section[ServerConfig]("server"))
}
