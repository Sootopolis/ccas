package ccas.api.player

import ccas.api.misc.subtypes.Username
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
case class ApiPlayerArchives(archives: Chunk[URL])

object ApiPlayerArchives extends JsonDecoding[ApiPlayerArchives] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayerArchives] = DeriveJsonDecoder.gen

  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("archives")
}
