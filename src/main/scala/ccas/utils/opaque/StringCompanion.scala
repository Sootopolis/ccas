package ccas.utils.opaque

import io.getquill.MappedEncoding
import zio.json.{JsonDecoder, JsonEncoder}

trait StringCompanion[S] {
  final inline def apply(string: String): S = fromStringUnsafe(string)

  protected def fromStringUnsafe(string: String): S
  protected def toStringUnsafe(opaque: S): String

  given jsonDecoder: JsonDecoder[S] = JsonDecoder.string.map(fromStringUnsafe)
  given jsonEncoder: JsonEncoder[S] = JsonEncoder.string.contramap(toStringUnsafe)
  given sqlDecoder: MappedEncoding[String, S] = MappedEncoding(fromStringUnsafe)
  given sqlEncoder: MappedEncoding[S, String] = MappedEncoding(toStringUnsafe)

  extension (opaque: S) {
    inline def length: Int = toStringUnsafe(opaque).length
    inline def toLowerCase: S = fromStringUnsafe(toStringUnsafe(opaque).toLowerCase)
    inline def toUpperCase: S = fromStringUnsafe(toStringUnsafe(opaque).toUpperCase)
  }
}
