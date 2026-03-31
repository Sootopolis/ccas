package ccas.utils.json

import zio.http.URL
import zio.json.JsonDecoder

object JsonDecoding {
  given JsonDecoder[URL] = JsonDecoder.string.mapOrFail(URL.decode(_).left.map(_.getMessage))
}
