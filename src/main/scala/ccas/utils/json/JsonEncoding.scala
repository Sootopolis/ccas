package ccas.utils.json

import zio.http.URL
import zio.json.{EncoderOps, JsonEncoder}
import zio.{UIO, ZIO}

import java.time.Instant

trait JsonEncoding[T] {
  protected val jsonEncoderDerived: JsonEncoder[T]

  final given jsonEncoder: JsonEncoder[T] = jsonEncoderDerived

  final given urlJsonEncoder: JsonEncoder[URL] = JsonEncoding.urlJsonEncoder

  final given instantJsonEncoder: JsonEncoder[Instant] = JsonEncoding.instantJsonEncoder

  extension (t: T) {
    def encode: String = t.toJsonPretty(using jsonEncoderDerived)

    def encodeZIO: UIO[String] = ZIO.succeed(encode)
  }
}

object JsonEncoding {
  private val urlJsonEncoder: JsonEncoder[URL] = JsonEncoder.string.contramap(_.encode)
  private val instantJsonEncoder: JsonEncoder[Instant] = JsonEncoder.long.contramap(_.getEpochSecond)
}