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
  averageDailyRating: Option[Elo], // absent on some clubs; can come and go on the same club over time
  membersCount: Int,               // total members count
  created: Long,                   // timestamp of creation on Chess.com
  lastActivity: Long,              // timestamp of the most recent post, match, etc
  visibility: ClubVisibility,      // whether the club is public or private
  joinRequest: URL,                // location to submit a request to join this club
  admin: Chunk[URL],               // array of URLs to the player profiles for the admins of this club
  description: Option[String]      // text description of the club
) derives JsonDecoder {
  /** The slug Chess.com answers to for this club right now, read from the self-referencing `@id` rather than from
    * whatever slug was requested — the two differ after a rename, and only this one is safe to persist.
    */
  def canonicalSlug: ClubSlug = ClubSlug.wrap(`@id`.path.segments.last)
}

object ApiClub {
  val host: URL = Hosts.api.addPath("club")

  def getUrl(clubSlug: ClubSlug): URL = host.addPath(clubSlug.value)

  def get(client: ChessComClient, clubSlug: ClubSlug): Task[ApiClub] = client.getUncached[ApiClub](getUrl(clubSlug))

  /** [[get]] for a slug that may not exist — a rename probe, say. `None` on 404, and that 404 is not a failure. */
  def getOptional(client: ChessComClient, clubSlug: ClubSlug): Task[Option[ApiClub]] =
    client.getUncachedOptional[ApiClub](getUrl(clubSlug))
}
