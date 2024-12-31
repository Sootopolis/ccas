package ccas.api.club

import ccas.api.club.ApiClubMembers.ApiClubMember
import ccas.api.utils.subtypes.{ClubUrlName, Username}
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiClubMembers(
  weekly : Chunk[ApiClubMember],
  monthly: Chunk[ApiClubMember],
  allTime: Chunk[ApiClubMember]
)

object ApiClubMembers extends JsonDecoding[ApiClubMembers] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiClubMembers] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  case class ApiClubMember(username: Username, joined: Instant) derives JsonDecoder

  def getUrl(clubUrlName: ClubUrlName): URL = ApiClub.getUrl(clubUrlName).addPath("members")
}
