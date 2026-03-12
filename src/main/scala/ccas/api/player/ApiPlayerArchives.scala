package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.misc.subtypes.Username
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiPlayerArchives(archives: Chunk[URL])

object ApiPlayerArchives extends JsonDecoding[ApiPlayerArchives] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerArchives] = DeriveJsonDecoder.gen

  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("archives")
}
