package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.api.player.ApiPlayerClubs.ApiPlayerClub
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiPlayerClubs(clubs: Chunk[ApiPlayerClub])

object ApiPlayerClubs extends JsonDecoding[ApiPlayerClubs] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerClubs] = DeriveJsonDecoder.gen

  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("clubs")

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerClub(name: String, lastActivity: Long, icon: Option[URL], url: URL, joined: Long)
      derives JsonDecoder {
    def clubName: ClubSlug = ClubSlug.wrap(url.path.segments.last)

    def clubApiUrl: URL = ApiClub.getUrl(clubName)
  }
}
