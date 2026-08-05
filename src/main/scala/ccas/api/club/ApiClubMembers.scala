package ccas.api.club

import zio.{Chunk, Task}
import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}

import ccas.api.club.ApiClubMembers.ApiClubMember
import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.utils.client.ChessComClient

@jsonMemberNames(SnakeCase)
final case class ApiClubMembers(
  weekly: Chunk[ApiClubMember],
  monthly: Chunk[ApiClubMember],
  allTime: Chunk[ApiClubMember]
) derives JsonDecoder {
  def all: Chunk[ApiClubMember] = weekly ++ monthly ++ allTime

  def toMap: Map[Username, Long] = all.map(member => member.username -> member.joined).toMap
}

object ApiClubMembers {
  @jsonMemberNames(SnakeCase)
  final case class ApiClubMember(username: Username, joined: Long) derives JsonDecoder

  def getUrl(clubSlug: ClubSlug): URL = ApiClub.getUrl(clubSlug).addPath("members")

  def get(client: ChessComClient, clubSlug: ClubSlug): Task[ApiClubMembers] =
    client.getUncached[ApiClubMembers](getUrl(clubSlug))
}
