package ccas.utils.json

import java.time.Instant
import scala.util.Try

import zio.{IO, ZIO}
import zio.http.URL
import zio.json.{DecoderOps, JsonDecoder}

trait JsonDecoding[T] {
  export JsonDecoding.given

  protected val jsonDecoderDerived: JsonDecoder[T]

  final given jsonDecoder: JsonDecoder[T] = jsonDecoderDerived

  final def decode(string: String): Either[String, T] = string.fromJson[T]

  final def decodeZIO(string: String): IO[JsonDecodingException, T] =
    ZIO.fromEither(decode(string)).mapError(JsonDecodingException(_))

  extension (string: String) {
    def decodeJson: Either[String, T]               = string.fromJson[T]
    def decodeJsonZIO: IO[JsonDecodingException, T] = ZIO.fromEither(decodeJson).mapError(JsonDecodingException(_))
  }
}

object JsonDecoding {
  given urlJsonDecoder: JsonDecoder[URL] = JsonDecoder.string.mapOrFail(URL.decode(_).left.map(_.getMessage))

  given instantJsonDecoder: JsonDecoder[Instant] = JsonDecoder.long.mapOrFail { long =>
    Try(Instant.ofEpochSecond(long)).toEither.left.map(_.getMessage)
  }
}
