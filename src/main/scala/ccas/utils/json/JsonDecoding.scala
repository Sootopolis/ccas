package ccas.utils.json

import zio.http.URL
import zio.json.{DecoderOps, JsonDecoder}
import zio.{IO, ZIO}

import java.time.Instant
import scala.util.Try

trait JsonDecoding[T] {
  protected val jsonDecoderDerived: JsonDecoder[T]

  final given jsonDecoder: JsonDecoder[T] = jsonDecoderDerived

  final given urlJsonDecoder: JsonDecoder[URL] = JsonDecoding.urlJsonDecoder

  final given instantJsonDecoder: JsonDecoder[Instant] = JsonDecoding.instantJsonDecoder

  final def decode(string: String): Either[String, T] = string.fromJson[T]

  final def decodeZIO(string: String): IO[JsonDecodingException, T] =
    ZIO.fromEither(decode(string)).mapError(JsonDecodingException(_))

  extension (string: String) {
    def decodeJson: Either[String, T] = string.fromJson[T]
    def decodeJsonZIO: IO[JsonDecodingException, T] = ZIO.fromEither(decodeJson).mapError(JsonDecodingException(_))
  }
}

object JsonDecoding {
  private val urlJsonDecoder: JsonDecoder[URL] = JsonDecoder.string.mapOrFail(URL.decode(_).left.map(_.getMessage))

  private val instantJsonDecoder: JsonDecoder[Instant] = JsonDecoder.long
    .mapOrFail(instant => Try(Instant.ofEpochSecond(instant)).toEither.left.map(_.getMessage))
}