package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.Chunk
import zio.Config.Error.InvalidData
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

trait IntCompanion {
  opaque type Type = Int

  def apply(value: Int): Type    = value
  def wrap(value: Int): Type     = value
  def unwrap(value: Type): Int   = value

  protected val name: String = getClass.getSimpleName.stripSuffix("$")

  protected def validateRaw(raw: Int): Either[String, Int] = Right(raw)
  protected def validated(raw: Int): Either[String, Type] = validateRaw(raw).map(wrap)

  given JsonCodec[Type]    = JsonCodec.int.transformOrFail(validated, unwrap)
  given JsonDecoder[Type]  = summon[JsonCodec[Type]].decoder
  given JsonEncoder[Type]  = summon[JsonCodec[Type]].encoder
  given DbCodec[Type]      = DbCodec[Int].biMap(wrap, unwrap)
  given DeriveConfig[Type] = DeriveConfig[Int].mapOrFail(validated(_).left.map(InvalidData(Chunk.empty, _)))

  extension (i: Type) {
    def value: Int = unwrap(i)
  }
}
