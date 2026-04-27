package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.JsonCodec

trait LongCompanion extends BaseNumericCompanion[Long] {
  opaque type Type = Long

  def apply(value: Long): Type  = value
  def wrap(value: Long): Type   = value
  def unwrap(value: Type): Long = value

  // Named codec refs (not `summon` / `DbCodec[Long]` apply) avoid an opaque-leak class-init deadlock: implicit
  // search would resolve `DbCodec[Long]` back to the inherited `given DbCodec[Type]` since `Type = Long`
  // opaquely, recursing into the lazy val being initialized. See IntCompanion for full context.
  protected def baseJsonCodec: JsonCodec[Long]       = JsonCodec.long
  protected def baseDbCodec: DbCodec[Long]           = DbCodec.LongCodec
  protected def baseDeriveConfig: DeriveConfig[Long] = DeriveConfig(zio.Config.long)
  protected def baseOrdering: Ordering[Long]         = Ordering.Long
}

/** `LongCompanion` with a `> 0L` validator. Use for surrogate keys backed by `BIGSERIAL`,
  * which always start at 1 — zero or negative values indicate corruption.
  */
trait PositiveLongCompanion extends LongCompanion {
  override protected def validateRaw(raw: Long): Either[String, Long] =
    Either.cond(raw > 0L, raw, s"$name must be > 0")
}
