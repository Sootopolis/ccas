package ccas.utils.json

import java.time.Instant

import zio.{UIO, ZIO}
import zio.http.URL
import zio.json.{EncoderOps, JsonEncoder}

trait JsonEncoding[T] {
  protected val jsonEncoderDerived: JsonEncoder[T]

  final given jsonEncoder: JsonEncoder[T] = jsonEncoderDerived

  final given urlJsonEncoder: JsonEncoder[URL] = JsonEncoding.urlJsonEncoder

  final given instantJsonEncoder: JsonEncoder[Instant] = JsonEncoding.instantJsonEncoder

  extension (value: T) {
    def encode: String = value.toJsonPretty(using jsonEncoderDerived)

    def encodeZIO: UIO[String] = ZIO.succeed(encode)
  }
}

object JsonEncoding {
  private val urlJsonEncoder: JsonEncoder[URL]         = JsonEncoder.string.contramap(_.encode)
  private val instantJsonEncoder: JsonEncoder[Instant] = JsonEncoder.long.contramap(_.getEpochSecond)
}
