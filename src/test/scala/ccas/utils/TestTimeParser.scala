package ccas.utils

import java.time.Instant

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

object TestTimeParser extends ZIOSpecDefault {

  override def spec: Spec[Any, Nothing] = suite("TimeParser.parseInstant")(
    test("parses full ISO instant") {
      assertTrue(TimeParser.parseInstant("2026-03-23T00:00:00Z") == Right(Instant.parse("2026-03-23T00:00:00Z")))
    },
    test("parses plain date as midnight UTC") {
      assertTrue(TimeParser.parseInstant("2026-03-23") == Right(Instant.parse("2026-03-23T00:00:00Z")))
    },
    test("parses instant with non-zero time") {
      assertTrue(TimeParser.parseInstant("2026-03-23T14:30:00Z") == Right(Instant.parse("2026-03-23T14:30:00Z")))
    },
    test("rejects invalid string") {
      assertTrue(TimeParser.parseInstant("not-a-date").isLeft)
    },
    test("rejects datetime without offset") {
      assertTrue(TimeParser.parseInstant("2026-03-23T00:00:00").isLeft)
    }
  )
}
