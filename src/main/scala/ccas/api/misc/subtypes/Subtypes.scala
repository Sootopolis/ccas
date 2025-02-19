package ccas.api.misc.subtypes

import zio.{Config, NonEmptyChunk}
import zio.config.magnolia.DeriveConfig
import zio.json.{JsonCodec, JsonFieldDecoder, JsonFieldEncoder}
import zio.prelude.{Assertion, Subtype}

private def stringError(errors: NonEmptyChunk[String]) = errors.mkString("\n")
private def configError(errors: NonEmptyChunk[String]) = Config.Error.InvalidData(message = stringError(errors))

type Elo = Elo.Type

object Elo extends Subtype[Int] {
  override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

  given jsonCodec: JsonCodec[Elo] = derive

  given derivedConfig: DeriveConfig[Elo] = DeriveConfig.from(Config.int).mapOrFail(make(_).toEitherWith(configError))
}

type PlayerId = PlayerId.Type

object PlayerId extends Subtype[Int] {
  override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

  given jsonCodec: JsonCodec[PlayerId] = derive
}

type Username = Username.Type

object Username extends Subtype[String] {
  override def assertion: Assertion[String] = !Assertion.isEmptyString

  given jsonCodec: JsonCodec[Username] = derive

  given fieldDecoder: JsonFieldDecoder[Username] = derive

  given fieldEncoder: JsonFieldEncoder[Username] = derive

  given derivedConfig: DeriveConfig[Username] = derive
}

type ClubId = ClubId.Type

object ClubId extends Subtype[Int] {
  override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

  given jsonCodec: JsonCodec[ClubId] = derive

  given derivedConfig: DeriveConfig[ClubId] = derive
}

type ClubUrlName = ClubUrlName.Type

object ClubUrlName extends Subtype[String] {
  override def assertion: Assertion[String] = !Assertion.isEmptyString

  given jsonCodec: JsonCodec[ClubUrlName] = derive

  given fieldDecoder: JsonFieldDecoder[ClubUrlName] = derive

  given fieldEncoder: JsonFieldEncoder[ClubUrlName] = derive

  given derivedConfig: DeriveConfig[ClubUrlName] = derive
}

type ClubMatchId = ClubMatchId.Type

object ClubMatchId extends Subtype[Int] {
  override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

  given jsonCodec: JsonCodec[ClubMatchId] = derive

  given fieldDecoder: JsonFieldDecoder[ClubMatchId] = derive

  given fieldEncoder: JsonFieldEncoder[ClubMatchId] = derive
}

type Rate = Rate.Type

object Rate extends Subtype[Double] {
  override def assertion: Assertion[Double] = Assertion.between(0.0, 1.0)

  given jsonCodec: JsonCodec[Rate] = derive

  given derivedConfig: Config[Rate] = Config.double.mapOrFail(make(_).toEitherWith(configError))
}
