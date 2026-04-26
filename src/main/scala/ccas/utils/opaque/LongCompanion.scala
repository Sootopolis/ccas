package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import zio.Chunk
import zio.Config.Error.InvalidData

import ccas.utils.opaque.OpaqueHelpers.orThrowDbRead

trait LongCompanion {
  opaque type Type = Long

  def apply(value: Long): Type  = value
  def wrap(value: Long): Type   = value
  def unwrap(value: Type): Long = value

  protected val name: String = getClass.getSimpleName.stripSuffix("$")

  protected def validateRaw(raw: Long): Either[String, Long] = Right(raw)
  protected def validated(raw: Long): Either[String, Type]   = validateRaw(raw).map(wrap)

  given JsonCodec[Type]    = JsonCodec.long.transformOrFail(validated, unwrap)
  given JsonDecoder[Type]  = summon[JsonCodec[Type]].decoder
  given JsonEncoder[Type]  = summon[JsonCodec[Type]].encoder
  given DbCodec[Type] = DbCodec[Long].biMap(raw => validated(raw).orThrowDbRead(name), unwrap)
  given DeriveConfig[Type] = DeriveConfig[Long].mapOrFail(validated(_).left.map(InvalidData(Chunk.empty, _)))
  given Ordering[Type]     = Ordering.by(unwrap)

  extension (l: Type) {
    def value: Long = unwrap(l)
  }
}

/** `LongCompanion` with a `> 0L` validator. Use for surrogate keys backed by `BIGSERIAL`,
  * which always start at 1 — zero or negative values indicate corruption.
  */
trait PositiveLongCompanion extends LongCompanion {
  override protected def validateRaw(raw: Long): Either[String, Long] =
    Either.cond(raw > 0L, raw, s"$name must be > 0")
}
