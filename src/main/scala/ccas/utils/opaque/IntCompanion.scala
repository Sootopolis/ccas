package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.JsonCodec

trait IntCompanion extends BaseNumericCompanion[Int] {
  opaque type Type = Int

  def apply(value: Int): Type  = value
  def wrap(value: Int): Type   = value
  def unwrap(value: Type): Int = value

  // Named refs (not summon/apply) so implicit search doesn't find the inherited `given DbCodec[Type]`
  // which erases to `DbCodec[Int]` opaquely — that would deadlock the lazy val at class init.
  protected def baseJsonCodec: JsonCodec[Int]       = JsonCodec.int
  protected def baseDbCodec: DbCodec[Int]           = DbCodec.IntCodec
  protected def baseDeriveConfig: DeriveConfig[Int] = DeriveConfig(zio.Config.int)
  protected def baseOrdering: Ordering[Int]         = Ordering.Int
}
