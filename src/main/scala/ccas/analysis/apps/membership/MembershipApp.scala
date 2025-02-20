package ccas.analysis.apps.membership

import ccas.analysis.tables.clubadmin.ClubMember
import ccas.analysis.tables.clubadmin.ClubMember.LatestMembershipRecords
import ccas.api.club.ApiClubMembers
import ccas.api.club.ApiClubMembers.ApiClubMember
import ccas.api.misc.subtypes.Username
import ccas.utils.client.CcasClient
import ccas.utils.configs.BaseClubConfig.ClubConfig
import zio.{Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

import java.time.Instant

object MembershipApp extends ZIOAppDefault {
  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] = ???

  def updateLatest(client: CcasClient, clubConfig: ClubConfig) = {
    val x = for {
      apiClubMembersByUsername <- ApiClubMembers.get(client, clubConfig.clubUrlName).map(_.toMap)
      records <- ClubMember.loadLatestForClub(clubConfig.clubId)
    } yield ()
    ???
  }

  def processApiMembers(
    apiMembersByUsername: Map[Username, Long],
    records: LatestMembershipRecords
  ): Task[ClubMember] = ???

  def processApiMember(
    username: Username,
    since: Instant,
    records: LatestMembershipRecords
  ): Task[ClubMember] = ???
}
