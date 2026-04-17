package ccas.utils

import java.time.Instant

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

object TestHttpDate extends ZIOSpecDefault {

  // 2026-04-16 23:13:22 UTC is a Thursday.
  private val instant = Instant.parse("2026-04-16T23:13:22Z")

  override def spec: Spec[Any, Nothing] = suite("HttpDate.parse")(
    test("parses Chess.com non-RFC format") {
      assertTrue(HttpDate.parse("Thursday, 16-Apr-2026 23:13:22 GMT+0000").contains(instant))
    },
    test("parses Chess.com format with negative offset") {
      // 19:13:22 at GMT-0400 is the same moment as 23:13:22 UTC.
      assertTrue(HttpDate.parse("Thursday, 16-Apr-2026 19:13:22 GMT-0400").contains(instant))
    },
    test("parses IMF-fixdate (RFC 7231 preferred)") {
      assertTrue(HttpDate.parse("Thu, 16 Apr 2026 23:13:22 GMT").contains(instant))
    },
    test("parses RFC 850 with 2-digit year") {
      assertTrue(HttpDate.parse("Thursday, 16-Apr-26 23:13:22 GMT").contains(instant))
    },
    test("parses asctime with space-padded single-digit day") {
      // 1994-11-06 was a Sunday.
      assertTrue(HttpDate.parse("Sun Nov  6 08:49:37 1994").contains(Instant.parse("1994-11-06T08:49:37Z")))
    },
    test("parses asctime with 2-digit day") {
      // 1994-11-16 was a Wednesday.
      assertTrue(HttpDate.parse("Wed Nov 16 08:49:37 1994").contains(Instant.parse("1994-11-16T08:49:37Z")))
    },
    test("returns None for empty string") {
      assertTrue(HttpDate.parse("").isEmpty)
    },
    test("returns None for garbage") {
      assertTrue(HttpDate.parse("not a date").isEmpty)
    },
    test("returns None for partial match") {
      assertTrue(HttpDate.parse("Thursday, 16-Apr-2026").isEmpty)
    },
    // The RFC_1123_DATE_TIME formatter uses parseLenient but the default SMART resolver still flags weekday/date
    // mismatches as inconsistent. The Chess.com formatter is STRICT by default. We lock in both behaviours below so
    // a future JDK upgrade that quietly relaxes either one gets caught here.
    test("returns None when weekday contradicts the date (Chess.com format)") {
      // 2026-04-16 is a Thursday, not Friday.
      assertTrue(HttpDate.parse("Friday, 16-Apr-2026 23:13:22 GMT+0000").isEmpty)
    },
    test("returns None when weekday contradicts the date (IMF-fixdate)") {
      assertTrue(HttpDate.parse("Fri, 16 Apr 2026 23:13:22 GMT").isEmpty)
    }
  )
}
