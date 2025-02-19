package ccas.analysis.apps.membership

import ccas.analysis.tables.clubadmin.ClubMember
import ccas.api.club.{ApiClub, ApiClubMembers}
import ccas.utils.client.CcasClient
import ccas.utils.configs.ClubConfig
import zio.{Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

object MembershipApp extends ZIOAppDefault {
  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] = ???

  def updateLatest(client: CcasClient, clubConfig: ClubConfig) = {
    val x = for {
      _ <- clubConfig.checkClubId(client)
      apiClubMembersByUsername <- ApiClubMembers.get(client, clubConfig.clubUrlName).map(_.toMap)
    } yield ()
    ???
  }
}
