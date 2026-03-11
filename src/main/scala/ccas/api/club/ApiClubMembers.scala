package ccas.api.club

import ccas.api.club.ApiClubMembers.ApiClubMember
import ccas.api.misc.subtypes.{ClubUrlName, Username}
import ccas.utils.client.CcasClient
import ccas.utils.json.JsonDecoding
import zio.{Chunk, Task}
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
final case class ApiClubMembers(
  weekly : Chunk[ApiClubMember],
  monthly: Chunk[ApiClubMember],
  allTime: Chunk[ApiClubMember]
) {
  def toIterator: Iterator[ApiClubMember] = weekly.iterator ++ monthly.iterator ++ allTime.iterator

  def all: Chunk[ApiClubMember] = Chunk.fromIterator(toIterator)

  def toMap: Map[Username, Long] = all.map(member => member.username -> member.joined).toMap
}

object ApiClubMembers extends JsonDecoding[ApiClubMembers] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiClubMembers] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  final case class ApiClubMember(username: Username, joined: Long) derives JsonDecoder

  def getUrl(clubUrlName: ClubUrlName): URL = ApiClub.getUrl(clubUrlName).addPath("members")

  def get(client: CcasClient, clubUrlName: ClubUrlName): Task[ApiClubMembers] =
    client.get[ApiClubMembers](getUrl(clubUrlName))
}
