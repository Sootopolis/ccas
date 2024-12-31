package ccas.api.club

import ccas.api.utils.Hosts
import ccas.api.utils.enums.ClubVisibility
import ccas.api.utils.subtypes.{ClubId, ClubUrlName, Elo}
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiClub(
  `@id`             : URL, // the location of this profile (always self-referencing)
  name              : String, // the human-readable name of this club
  clubId            : ClubId, // the non-changing Chess.com ID of this club
  icon              : Option[URL], // (optional) URL of a 200x200 image
  country           : URL, // location of this club's country profile
  location          : Option[String],
  averageDailyRating: Elo, //average daily rating
  membersCount      : Int, //total members count
  created           : Instant, // timestamp of creation on Chess.com
  lastActivity      : Instant, // timestamp of the most recent post, match, etc
  visibility        : ClubVisibility, // whether the club is public or private
  joinRequest       : URL, // location to submit a request to join this club
  admin             : Chunk[URL], // array of URLs to the player profiles for the admins of this club
  description       : String // text description of the club
)

object ApiClub extends JsonDecoding[ApiClub] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiClub] = DeriveJsonDecoder.gen

  val host: URL = Hosts.api.addPath("club")

  def getUrl(clubUrlName: ClubUrlName): URL = host.addPath(clubUrlName)
}
