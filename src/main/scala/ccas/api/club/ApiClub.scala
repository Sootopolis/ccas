package ccas.api.club

import ccas.api.Hosts
import ccas.api.club.ApiClub.ClubVisibility
import ccas.api.utils.Subtypes.{ClubId, ClubName, Elo}
import zio.Chunk
import zio.http.URL

case class ApiClub(
  `@id`             : URL, // the location of this profile (always self-referencing)
  name              : String, // the human-readable name of this club
  clubId            : ClubId, // the non-changing Chess.com ID of this club
  icon              : Option[URL], // (optional) URL of a 200x200 image
  country           : URL, // location of this club's country profile
  location          : Option[String],
  averageDailyRating: Elo, //average daily rating
  membersCount      : Int, //total members count
  created           : 1178556600, // timestamp of creation on Chess.com
  lastActivity      : 1500661803, // timestamp of the most recent post, match, etc
  visibility        : ClubVisibility, // whether the club is public or private
  joinRequest       : URL, // location to submit a request to join this club
  admin             : Chunk[URL], // array of URLs to the player profiles for the admins of this club
  description       : String // text description of the club
)

object ApiClub {
  val host: URL = Hosts.api.addPath("club")

  def getUrl(clubName: ClubName): URL = host.addPath(clubName)

  enum ClubVisibility {
    case public
    case `private`
  }
}
