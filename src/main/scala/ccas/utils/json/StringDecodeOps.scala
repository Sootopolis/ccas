package ccas.utils.json

import zio.{IO, ZIO}
import zio.json.{DecoderOps, JsonDecoder}

private[json] trait StringDecodeOps[T] {
  protected def jsonDecoderInstance: JsonDecoder[T]

  extension (string: String) {
    def decodeJson: Either[String, T]               = string.fromJson[T](using jsonDecoderInstance)
    def decodeJsonZIO: IO[JsonDecodingException, T] = ZIO.fromEither(decodeJson).mapError(JsonDecodingException(_))
  }
}
