package ccas.api.player

import ccas.api.club.ApiClub
import ccas.api.player.ApiPlayerClubs.ApiPlayerClub
import ccas.api.utils.Subtypes.ClubName
import ccas.utils.PrettyPrinting
import zio.Chunk
import zio.http.URL

import java.time.Instant

case class ApiPlayerClubs(clubs: Chunk[ApiPlayerClub]) extends PrettyPrinting[ApiPlayerClubs]

object ApiPlayerClubs {
  case class ApiPlayerClub(
    name        : String,
    lastActivity: Instant,
    icon        : Option[URL],
    url         : URL,
    joined      : Instant
  ) {
    def clubName: ClubName = ClubName.wrap(url.path.segments.last)
    
    def clubApiUrl: URL = ApiClub.getUrl(clubName)
  }
}
