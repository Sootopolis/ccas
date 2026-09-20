package ccas.cli

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Pins the `recruit` flag combinations refused before anything reaches the server. */
object TestRecruitUsage extends ZIOSpecDefault {

  override def spec: Spec[Any, Nothing] = suite("TestRecruitUsage")(
    test("a run id without --report is refused, since it would launch a fresh scout instead") {
      val refused = Dispatcher.recruitUsageError(report = false, runId = Some(42), club = None, clubIdOption = None)
      assertTrue(refused.isDefined)
    },
    test("a run id beside --club or --club-id is refused rather than silently overriding the club") {
      assertTrue(
        Dispatcher
          .recruitUsageError(report = true, runId = Some(42), club = Some("team-alpha"), clubIdOption = None)
          .isDefined,
        Dispatcher.recruitUsageError(report = true, runId = Some(42), club = None, clubIdOption = Some(621L)).isDefined
      )
    },
    test("a report names its run or its club, and a scout takes a club, without complaint") {
      assertTrue(
        Dispatcher.recruitUsageError(report = true, runId = Some(42), club = None, clubIdOption = None).isEmpty,
        Dispatcher
          .recruitUsageError(report = true, runId = None, club = Some("team-alpha"), clubIdOption = None)
          .isEmpty,
        Dispatcher.recruitUsageError(report = false, runId = None, club = None, clubIdOption = Some(621L)).isEmpty
      )
    }
  )
}
