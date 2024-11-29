import zio.prelude.{Assertion, Subtype, Validation}

object EloPlayground extends App {
  type Elo = Elo.Type // `Type` in `Subtype[A]` is defined as `type Type <: A`

  object Elo extends Subtype[Int] {
    override inline def assertion: Assertion[Int] = Assertion.greaterThanOrEqualTo(0)

    val magnusElo: Elo = Elo(2882)
  }

  val int: Int = 1598

  /**
   * .wrap
   * Accepts literal and non-literal values.
   * Bypasses assertion entirely.
   * Returns an `Elo`.
   */
  val wrappedElo: Elo = Elo.wrap(int)
  val wrappedBadElo: Elo = Elo.wrap(-int) // compiles and runs, but please don't do this

  /**
   * .apply
   * Requires `def assertion` to be either not overridden or overridden with `inlined` prefix.
   * Only accepts literal values.
   * Checks assertion at compile time.
   * Returns an `Elo`.
   */
  val myElo = Elo(1598)
  // val appliedBadElo = Elo(-1598) // doesn't compile because assertion fails
  // val failedAppliedElo = Elo(int) // doesn't compile because `magnusElo` is not literal
  inline def appliedNonLiteral = Elo(int) // compiles, but cannot be used in a non-inlined context i.e. whatsoever


  case class EloError(message: String) extends Exception(message)

  /**
   * .make
   * Accepts literal and non-literal values.
   * Checks assertion at run time.
   * Returns a `ZValidation[Nothing, String, Elo]`.
   * The idiomatic ZIO way to instantiate `Subtype` instances.
   */
  val madeElo: Validation[String, Elo] = Elo.make(int)
  val madeBadElo: Validation[EloError, Elo] = Elo.make(-int).mapError(EloError(_))

  val eloAsInt: Int = Elo.magnusElo // works because Elo <: Int
  // val intAsElo: Elo = int // doesn't compile

  val diff: Int = Elo.magnusElo - myElo // `Int`, probably because assertion cannot be checked at compile time.
  val newElo: Int = myElo - 7 // has all operations of `Int` because Elo <: Int.
  val equality = Elo.magnusElo == 2882 // true

  trait Reader[T] {
    def read(s: String): T
  }

  val intReader = new Reader[Int] {
    override def read(s: String): Int = s.toInt
  }
  val eloReader: Reader[Elo] = Elo.derive[Reader](using intReader) // derives typeclass using that of parent class
  val eloFromString: Elo = eloReader.read("2882")
}
