package ccas.api.club

import ccas.api.utils.Subtypes.{ClubName, Username}
import ccas.utils.PrettyPrinting
import zio.Chunk
import zio.http.URL

import java.time.Instant

case class ApiClubMembers(weekly: Chunk[ApiClubMembers], monthly: Chunk[ApiClubMembers], allTime: Chunk[ApiClubMembers])
  extends PrettyPrinting[ApiClubMembers]

object ApiClubMembers {
  case class ApiClubMember(username: Username, joined: Instant)

  def getUrl(clubName: ClubName): URL = ApiClub.getUrl(clubName).addPath("members")
}
