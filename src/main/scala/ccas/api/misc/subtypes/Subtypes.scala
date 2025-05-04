package ccas.api.misc.subtypes

import io.getquill.MappedEncoding
import zio.Config.Error.InvalidData
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonFieldDecoder, JsonFieldEncoder}
import zio.prelude.{Assertion, Subtype}
import zio.{Chunk, Config, NonEmptyChunk, json}

sealed trait CcasSubtype[T: {JsonCodec, DeriveConfig}] extends Subtype[T] { self =>
  private val name = self.getClass.getSimpleName.stripSuffix("$")

  protected def validated(value: Type): Either[String, Type] =
    make(value).toEitherWith(errors => errors.mkString(s"Error validating $name:\n  ", "\n  ", ""))

  given MappedEncoding[Type, T] = MappedEncoding(unwrap)
  given MappedEncoding[T, Type] = MappedEncoding(wrap)
  given JsonCodec[Type] = derive[JsonCodec].transformOrFail(validated, identity)
  given DeriveConfig[Type] = derive[DeriveConfig].mapOrFail(validated(_).left.map(InvalidData(Chunk.empty, _)))
}

sealed trait CcasKeySubtype[T: {JsonCodec, DeriveConfig, JsonFieldEncoder, JsonFieldDecoder}] extends CcasSubtype[T] {
  given JsonFieldEncoder[Type] = derive[JsonFieldEncoder]
  given JsonFieldDecoder[Type] = derive[JsonFieldDecoder].mapOrFail(validated)
}

type Elo = Elo.Type

object Elo extends CcasSubtype[Int] {
  override inline def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)
}

type PlayerId = PlayerId.Type

object PlayerId extends CcasSubtype[Long] {
  override inline def assertion: Assertion[Long] = Assertion.greaterThanOrEqualTo(0L)
}

type Username = Username.Type

object Username extends CcasKeySubtype[String] {
  override inline def assertion: Assertion[String] = !Assertion.isEmptyString
}

type ClubId = ClubId.Type

object ClubId extends CcasSubtype[Long] {
  override inline def assertion: Assertion[Long] = Assertion.greaterThanOrEqualTo(0L)
}

type ClubUrlName = ClubUrlName.Type

object ClubUrlName extends CcasKeySubtype[String] {
  override inline def assertion: Assertion[String] = !Assertion.isEmptyString
}

type ClubMatchId = ClubMatchId.Type

object ClubMatchId extends CcasSubtype[Long] {
  override inline def assertion: Assertion[Long] = Assertion.greaterThanOrEqualTo(0L)
}

type Percentage = Percentage.Type

object Percentage extends CcasSubtype[Double] {
  override inline def assertion: Assertion[Double] = Assertion.between(0.0, 1.0)
}

type ClubAlias = ClubAlias.Type

object ClubAlias extends CcasSubtype[String] {
  override inline def assertion: Assertion[String] = !Assertion.isEmptyString
}
