package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.JsonCodec
import zio.Chunk
import zio.Config.Error.InvalidData

trait ShortCompanion extends BaseNumericCompanion[Short] {
  opaque type Type = Short

  def apply(value: Short): Type  = value
  def wrap(value: Short): Type   = value
  def unwrap(value: Type): Short = value

  // Short has no native JSON / Config wire — bridge through Int with a range check.
  // The base trait then composes `validateRaw` on top of these.
  protected def baseJsonCodec: JsonCodec[Short] = JsonCodec.int.transformOrFail(
    i =>
      if (i < Short.MinValue || i > Short.MaxValue) Left(s"$name value $i out of Short range")
      else Right(i.toShort),
    _.toInt
  )
  // Named codec refs (not `summon` / `DbCodec[Short]` apply) avoid an opaque-leak class-init deadlock: implicit
  // search would resolve `DbCodec[Short]` back to the inherited `given DbCodec[Type]` since `Type = Short`
  // opaquely, recursing into the lazy val being initialized. See IntCompanion for full context.
  protected def baseDbCodec: DbCodec[Short] = DbCodec.ShortCodec
  protected def baseDeriveConfig: DeriveConfig[Short] = DeriveConfig(zio.Config.int).mapOrFail { i =>
    if (i < Short.MinValue || i > Short.MaxValue) Left(InvalidData(Chunk.empty, s"$name value $i out of Short range"))
    else Right(i.toShort)
  }
  protected def baseOrdering: Ordering[Short] = Ordering.Short
}
