package ccas.api.club

import zio.{Chunk, Task}
import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}

import ccas.api.misc.enums.ClubVisibility
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Elo}
import ccas.api.misc.Hosts
import ccas.utils.client.ChessComClient
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiClub(
  `@id`: URL,        // the location of this profile (always self-referencing)
  name: String,      // the human-readable name of this club
  clubId: ClubId,    // the non-changing Chess.com ID of this club
  icon: Option[URL], // (optional) URL of a 200x200 image
  country: URL,      // location of this club's country profile
  location: Option[String],
  averageDailyRating: Elo,    // average daily rating
  membersCount: Int,          // total members count
  created: Long,              // timestamp of creation on Chess.com
  lastActivity: Long,         // timestamp of the most recent post, match, etc
  visibility: ClubVisibility, // whether the club is public or private
  joinRequest: URL,           // location to submit a request to join this club
  admin: Chunk[URL],          // array of URLs to the player profiles for the admins of this club
  description: Option[String] // text description of the club
) derives JsonDecoder

object ApiClub {
  val host: URL = Hosts.api.addPath("club")

  def getUrl(clubSlug: ClubSlug): URL = host.addPath(clubSlug.value)

  def get(client: ChessComClient, clubSlug: ClubSlug): Task[ApiClub] = client.get[ApiClub](getUrl(clubSlug))
}
