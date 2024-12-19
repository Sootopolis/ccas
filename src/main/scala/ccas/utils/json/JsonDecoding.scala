package ccas.utils.json

import zio.http.URL
import zio.json.JsonDecoder
import zio.{IO, ZIO}

object JsonDecoding {
  def decode[T](raw: String)(using decoder: JsonDecoder[T]): Either[JsonDecodingError, T] =
    decoder.decodeJson(raw).left.map(JsonDecodingError(_))

  def decodeZIO[T](raw: String)(using JsonDecoder[T]): IO[JsonDecodingError, T] = ZIO.fromEither(decode(raw))

  case class JsonDecodingError(message: String) extends Exception(message)

  given urlJsonDecoder: JsonDecoder[URL] = JsonDecoder.string.map(URL.decode(_).fold(throw _, identity))
}
