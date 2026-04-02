package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import zio.Chunk
import zio.Config.Error.InvalidData

trait StringCompanion {
  opaque type Type = String

  protected def normalize(raw: String): String = raw

  def apply(value: String): Type  = normalize(value)
  def wrap(value: String): Type   = if (value == null) value else normalize(value)
  def unwrap(value: Type): String = value

  protected val name: String = getClass.getSimpleName.stripSuffix("$")

  protected def validateRaw(raw: String): Either[String, String] = Right(raw)

  protected def validated(raw: String): Either[String, Type] = {
    val n = normalize(raw)
    validateRaw(n).map(_ => n)
  }

  given JsonCodec[Type]    = JsonCodec.string.transformOrFail(validated, unwrap)
  given JsonDecoder[Type]  = summon[JsonCodec[Type]].decoder
  given JsonEncoder[Type]  = summon[JsonCodec[Type]].encoder
  given DbCodec[Type]      = DbCodec[String].biMap(wrap, unwrap)
  given DeriveConfig[Type] = DeriveConfig[String].mapOrFail(validated(_).left.map(InvalidData(Chunk.empty, _)))
  given Ordering[Type]     = Ordering.by(unwrap)

  extension (s: Type) {
    def value: String                          = unwrap(s)
    def length: Int                            = unwrap(s).length
    def toLowerCase: Type                      = wrap(unwrap(s).toLowerCase)
    def toUpperCase: Type                      = wrap(unwrap(s).toUpperCase)
    def equalsIgnoreCase(other: Type): Boolean = unwrap(s).equalsIgnoreCase(unwrap(other))
  }
}
