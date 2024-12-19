package ccas.api.utils

import zio.json.JsonCodec
import zio.prelude.{Assertion, Subtype}

object Subtypes {
  type Elo = Elo.Type

  object Elo extends Subtype[Int] {
    override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

    given eloJsonCodec: JsonCodec[Elo] = derive
  }

  type PlayerId = PlayerId.Type

  object PlayerId extends Subtype[Int] {
    override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

    given playerIdJsonCodec: JsonCodec[PlayerId] = derive
  }

  type Username = Username.Type

  object Username extends Subtype[String] {
    override def assertion: Assertion[String] = !Assertion.isEmptyString

    given usernameJsonCodec: JsonCodec[Username] = derive
  }

  type ClubId = ClubId.Type

  object ClubId extends Subtype[Int] {
    override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

    given clubIdJsonCodec: JsonCodec[ClubId] = derive
  }

  type ClubUrlName = ClubUrlName.Type

  object ClubUrlName extends Subtype[String] {
    override def assertion: Assertion[String] = !Assertion.isEmptyString

    given clubNameJsonCodec: JsonCodec[ClubUrlName] = derive
  }

  type ClubMatchId = ClubMatchId.Type

  object ClubMatchId extends Subtype[Int] {
    override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

    given clubMatchIdJsonCodec: JsonCodec[ClubMatchId] = derive
  }
}
