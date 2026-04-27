package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.JsonCodec

trait DoubleCompanion extends BaseNumericCompanion[Double] {
  opaque type Type = Double

  def apply(value: Double): Type  = value
  def wrap(value: Double): Type   = value
  def unwrap(value: Type): Double = value

  // Named codec refs (not `summon` / `DbCodec[Double]` apply) avoid an opaque-leak class-init deadlock: implicit
  // search would resolve `DbCodec[Double]` back to the inherited `given DbCodec[Type]` since `Type = Double`
  // opaquely, recursing into the lazy val being initialized. See IntCompanion for full context.
  protected def baseJsonCodec: JsonCodec[Double]       = JsonCodec.double
  protected def baseDbCodec: DbCodec[Double]           = DbCodec.DoubleCodec
  protected def baseDeriveConfig: DeriveConfig[Double] = DeriveConfig(zio.Config.double)
  protected def baseOrdering: Ordering[Double]         = Ordering.Double.TotalOrdering
}
