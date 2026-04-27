package ccas.utils.opaque

import com.augustnagro.magnum.DbCodec
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import zio.Chunk
import zio.Config.Error.InvalidData

import ccas.utils.opaque.OpaqueHelpers.orThrowDbRead

/** Shared boilerplate for numeric opaque-type companions. Concrete companions (`IntCompanion`, `LongCompanion`,
  * `DoubleCompanion`, `ShortCompanion`) declare their own `opaque type Type = T` and the `wrap`/`unwrap` reveals;
  * this trait derives the JSON / DB / Config / Ordering givens around them. Subclasses fill in `baseJsonCodec` /
  * `baseDbCodec` / `baseDeriveConfig` / `baseOrdering` — overriding them lets non-homomorphic wire types work
  * (e.g. `ShortCompanion` uses an `Int` JSON / Config wire with a range check).
  */
trait BaseNumericCompanion[T] {
  type Type

  def apply(value: T): Type
  def wrap(value: T): Type
  def unwrap(value: Type): T

  // `def` (not `val`) so the lookup happens on first call, after subclass linearization completes — avoids a
  // null-`name` race if a `given` body somehow gets resolved during init.
  protected def name: String = getClass.getSimpleName.stripSuffix("$")

  protected def validateRaw(raw: T): Either[String, T] = Right(raw)
  protected def validated(raw: T): Either[String, Type] = validateRaw(raw).map(wrap)

  protected def baseJsonCodec: JsonCodec[T]
  protected def baseDbCodec: DbCodec[T]
  protected def baseDeriveConfig: DeriveConfig[T]
  protected def baseOrdering: Ordering[T]

  given jsonCodec: JsonCodec[Type] = baseJsonCodec.transformOrFail(validated, unwrap)
  given JsonDecoder[Type]          = jsonCodec.decoder
  given JsonEncoder[Type]          = jsonCodec.encoder
  given DbCodec[Type]              = baseDbCodec.biMap(raw => validated(raw).orThrowDbRead(name), unwrap)
  given DeriveConfig[Type]         = baseDeriveConfig.mapOrFail(validated(_).left.map(InvalidData(Chunk.empty, _)))
  given Ordering[Type]             = Ordering.by(unwrap)(using baseOrdering)

  extension (v: Type) def value: T = unwrap(v)
}
