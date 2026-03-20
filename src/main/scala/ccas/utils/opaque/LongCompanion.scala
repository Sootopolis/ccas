package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.Chunk
import zio.Config.Error.InvalidData
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

trait LongCompanion {
  opaque type Type = Long

  def apply(value: Long): Type    = value
  def wrap(value: Long): Type     = value
  def unwrap(value: Type): Long   = value

  protected val name: String = getClass.getSimpleName.stripSuffix("$")

  protected def validateRaw(raw: Long): Either[String, Long] = Right(raw)
  protected def validated(raw: Long): Either[String, Type] = validateRaw(raw).map(wrap)

  given JsonCodec[Type]    = JsonCodec.long.transformOrFail(validated, unwrap)
  given JsonDecoder[Type]  = summon[JsonCodec[Type]].decoder
  given JsonEncoder[Type]  = summon[JsonCodec[Type]].encoder
  given DbCodec[Type]      = DbCodec[Long].biMap(wrap, unwrap)
  given DeriveConfig[Type] = DeriveConfig[Long].mapOrFail(validated(_).left.map(InvalidData(Chunk.empty, _)))

  extension (l: Type) {
    def value: Long = unwrap(l)
  }
}
