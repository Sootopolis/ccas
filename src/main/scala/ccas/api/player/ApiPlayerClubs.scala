package ccas.api.player

import ccas.api.club.ApiClub
import ccas.api.player.ApiPlayerClubs.ApiPlayerClub
import ccas.api.utils.Subtypes.ClubUrlName
import zio.Chunk
import zio.http.URL
import zio.json.{SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiPlayerClubs(clubs: Chunk[ApiPlayerClub])

object ApiPlayerClubs {
  case class ApiPlayerClub(
    name        : String,
    lastActivity: Instant,
    icon        : Option[URL],
    url         : URL,
    joined      : Instant
  ) {
    def clubName: ClubUrlName = ClubUrlName.wrap(url.path.segments.last)
    
    def clubApiUrl: URL = ApiClub.getUrl(clubName)
  }
}
