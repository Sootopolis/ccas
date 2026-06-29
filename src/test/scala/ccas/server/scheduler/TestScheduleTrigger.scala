package ccas.server.scheduler

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Pure tests for the 5-field unix -> 6-field cron4s normalization and the cron/timezone validators. */
object TestScheduleTrigger extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("TestScheduleTrigger")(
    test("normalize prepends a seconds field and maps unix day wildcards onto cron4s's `?` rule") {
      assertTrue(
        ScheduleTrigger.normalize("0 9 * * *") == Right("0 0 9 * * ?"),    // every day  -> blank the dow
        ScheduleTrigger.normalize("0 15 * * MON") == Right("0 0 15 ? * MON"), // weekday    -> blank the dom
        ScheduleTrigger.normalize("0 9 1 * *") == Right("0 0 9 1 * ?")        // day-of-month -> blank the dow
      )
    },
    test("normalize trusts a `?` the user typed directly") {
      assertTrue(ScheduleTrigger.normalize("0 15 ? * MON") == Right("0 0 15 ? * MON"))
    },
    test("normalize rejects the wrong number of fields") {
      assertTrue(
        ScheduleTrigger.normalize("0 9 * *").isLeft,
        ScheduleTrigger.normalize("0 9 * * * *").isLeft
      )
    },
    test("normalize rejects restricting both day-of-month and day-of-week") {
      assertTrue(ScheduleTrigger.normalize("0 9 1 * MON").isLeft)
    },
    test("validateCron returns the normalized 6-field string for a valid cron") {
      assertTrue(
        ScheduleTrigger.validateCron("0 9 * * *") == Right("0 0 9 * * ?"),
        ScheduleTrigger.validateCron("*/15 * * * *") == Right("0 */15 * * * ?")
      )
    },
    test("validateCron rejects an out-of-range field") {
      assertTrue(ScheduleTrigger.validateCron("99 9 * * *").isLeft)
    },
    test("validateZone accepts an IANA zone and rejects garbage") {
      assertTrue(
        ScheduleTrigger.validateZone("Europe/London").isRight,
        ScheduleTrigger.validateZone("UTC").isRight,
        ScheduleTrigger.validateZone("Mars/Phobos").isLeft
      )
    }
  )
}
