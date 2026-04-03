package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import zio.Chunk
import zio.Config.Error.InvalidData

trait ShortCompanion {
  opaque type Type = Short

  def apply(value: Short): Type  = value
  def wrap(value: Short): Type   = value
  def unwrap(value: Type): Short = value

  protected val name: String = getClass.getSimpleName.stripSuffix("$")

  protected def validateRaw(raw: Short): Either[String, Short] = Right(raw)
  protected def validated(raw: Short): Either[String, Type]    = validateRaw(raw).map(wrap)

  given JsonCodec[Type] = JsonCodec.int.transformOrFail(
    i =>
      if (i < Short.MinValue || i > Short.MaxValue) Left(s"$name value $i out of Short range")
      else validated(i.toShort),
    s => unwrap(s).toInt
  )
  given JsonDecoder[Type]  = summon[JsonCodec[Type]].decoder
  given JsonEncoder[Type]  = summon[JsonCodec[Type]].encoder
  given DbCodec[Type]      = DbCodec[Short].biMap(wrap, unwrap)
  given DeriveConfig[Type] = DeriveConfig[Int].mapOrFail { i =>
    if (i < Short.MinValue || i > Short.MaxValue) Left(InvalidData(Chunk.empty, s"$name value $i out of Short range"))
    else validated(i.toShort).left.map(InvalidData(Chunk.empty, _))
  }
  given Ordering[Type] = Ordering.by(unwrap)

  extension (s: Type) {
    def value: Short = unwrap(s)
  }
}
