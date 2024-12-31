package ccas.api.utils.subtypes

import zio.json.{JsonCodec, JsonFieldDecoder, JsonFieldEncoder}
import zio.prelude.{Assertion, Subtype}

type Elo = Elo.Type

object Elo extends Subtype[Int] {
  override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

  given jsonCodec: JsonCodec[Elo] = derive
  given fieldDecoder: JsonFieldDecoder[Elo] = derive
  given fieldEncoder: JsonFieldEncoder[Elo] = derive
}

type PlayerId = PlayerId.Type

object PlayerId extends Subtype[Int] {
  override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

  given jsonCodec: JsonCodec[PlayerId] = derive
  given fieldDecoder: JsonFieldDecoder[PlayerId] = derive
  given fieldEncoder: JsonFieldEncoder[PlayerId] = derive
}

type Username = Username.Type

object Username extends Subtype[String] {
  override def assertion: Assertion[String] = !Assertion.isEmptyString

  given jsonCodec: JsonCodec[Username] = derive
  given fieldDecoder: JsonFieldDecoder[Username] = derive
  given fieldEncoder: JsonFieldEncoder[Username] = derive
}

type ClubId = ClubId.Type

object ClubId extends Subtype[Int] {
  override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

  given jsonCodec: JsonCodec[ClubId] = derive
  given fieldDecoder: JsonFieldDecoder[ClubId] = derive
  given fieldEncoder: JsonFieldEncoder[ClubId] = derive
}

type ClubUrlName = ClubUrlName.Type

object ClubUrlName extends Subtype[String] {
  override def assertion: Assertion[String] = !Assertion.isEmptyString

  given jsonCodec: JsonCodec[ClubUrlName] = derive
  given fieldDecoder: JsonFieldDecoder[ClubUrlName] = derive
  given fieldEncoder: JsonFieldEncoder[ClubUrlName] = derive
}

type ClubMatchId = ClubMatchId.Type

object ClubMatchId extends Subtype[Int] {
  override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

  given jsonCodec: JsonCodec[ClubMatchId] = derive
  given fieldDecoder: JsonFieldDecoder[ClubMatchId] = derive
  given fieldEncoder: JsonFieldEncoder[ClubMatchId] = derive
}