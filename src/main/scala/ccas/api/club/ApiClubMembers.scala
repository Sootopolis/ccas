package ccas.api.club

import ccas.api.utils.Subtypes.{ClubUrlName, Username}
import zio.Chunk
import zio.http.URL
import zio.json.{SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiClubMembers(weekly: Chunk[ApiClubMembers], monthly: Chunk[ApiClubMembers], allTime: Chunk[ApiClubMembers])

object ApiClubMembers {
  case class ApiClubMember(username: Username, joined: Instant)

  def getUrl(clubUrlName: ClubUrlName): URL = ApiClub.getUrl(clubUrlName).addPath("members")
}
