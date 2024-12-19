package ccas.utils.json

import zio.http.URL
import zio.json.JsonCodec

trait JsonCodecs {
  given urlJsonCodec: JsonCodec[URL] = JsonCodec.string.transform(URL.decode(_).fold(throw _, identity), _.encode)
}

object JsonCodecs extends JsonCodecs
