package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.subtypes.Username
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiPlayerArchives(archives: Chunk[URL]) derives JsonDecoder

object ApiPlayerArchives {
  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("archives")
}
