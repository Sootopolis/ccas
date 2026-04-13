package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import zio.Chunk
import zio.Config.Error.InvalidData

import ccas.utils.opaque.OpaqueHelpers.orThrowDbRead

trait DoubleCompanion {
  opaque type Type = Double

  def apply(value: Double): Type  = value
  def wrap(value: Double): Type   = value
  def unwrap(value: Type): Double = value

  protected val name: String = getClass.getSimpleName.stripSuffix("$")

  protected def validateRaw(raw: Double): Either[String, Double] = Right(raw)
  protected def validated(raw: Double): Either[String, Type]     = validateRaw(raw).map(wrap)

  given JsonCodec[Type]    = JsonCodec.double.transformOrFail(validated, unwrap)
  given JsonDecoder[Type]  = summon[JsonCodec[Type]].decoder
  given JsonEncoder[Type]  = summon[JsonCodec[Type]].encoder
  given DbCodec[Type] = DbCodec[Double].biMap(raw => validated(raw).orThrowDbRead(name), unwrap)
  given DeriveConfig[Type] = DeriveConfig[Double].mapOrFail(validated(_).left.map(InvalidData(Chunk.empty, _)))
  given Ordering[Type]     = Ordering.by(unwrap)

  extension (d: Type) {
    def value: Double = unwrap(d)
  }
}
