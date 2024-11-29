package ccas.api.utils

import zio.prelude.{Assertion, Subtype}

object Subtypes {
  type Elo = Elo.Type

  object Elo extends Subtype[Int] {
    override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)
  }

  type PlayerId = PlayerId.Type

  object PlayerId extends Subtype[Int] {
    override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)
  }

  type Username = Username.Type

  object Username extends Subtype[String] {
    override def assertion: Assertion[String] = !Assertion.isEmptyString
  }

  type ClubId = ClubId.Type

  object ClubId extends Subtype[Int] {
    override def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)
  }

  type ClubName = ClubName.Type

  object ClubName extends Subtype[String] {
    override def assertion: Assertion[String] = !Assertion.isEmptyString
  }
}
