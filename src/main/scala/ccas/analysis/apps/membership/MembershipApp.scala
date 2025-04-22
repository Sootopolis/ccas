package ccas.analysis.apps.membership

import ccas.utils.client.CcasClient
import ccas.utils.configs.BaseClubConfig.ClubConfig
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

object MembershipApp extends ZIOAppDefault {
  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] = ???

  def updateLatest(client: CcasClient, clubConfig: ClubConfig) = {
    ???
  }

}
