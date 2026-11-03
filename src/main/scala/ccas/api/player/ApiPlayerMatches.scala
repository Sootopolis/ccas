package ccas.api.player

import zio.Chunk
import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}

import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiPlayerMatches(
  finished: Chunk[ApiPlayerMatch],
  inProgress: Chunk[ApiPlayerMatch],
  registered: Chunk[ApiPlayerMatch]
) derives JsonDecoder

object ApiPlayerMatches {
  def getUrl(username: Username): URL = ApiPlayer.getUrl(username).addPath("matches")

  @jsonMemberNames(SnakeCase)
  final case class ApiPlayerMatch(
    name: String,
    url: URL,
    `@id`: URL,
    club: URL,
    board: Option[URL]
  ) derives JsonDecoder
}
