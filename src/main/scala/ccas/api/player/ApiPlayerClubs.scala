package ccas.api.player

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.ClubUrlName
import ccas.api.player.ApiPlayerClubs.ApiPlayerClub
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
case class ApiPlayerClubs(clubs: Chunk[ApiPlayerClub])

object ApiPlayerClubs extends JsonDecoding[ApiPlayerClubs] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerClubs] = DeriveJsonDecoder.gen

  @jsonMemberNames(SnakeCase)
  case class ApiPlayerClub(
    name        : String,
    lastActivity: Long,
    icon        : Option[URL],
    url         : URL,
    joined      : Long
  ) derives JsonDecoder {
    def clubName: ClubUrlName = ClubUrlName.wrap(url.path.segments.last)
    
    def clubApiUrl: URL = ApiClub.getUrl(clubName)
  }
}
