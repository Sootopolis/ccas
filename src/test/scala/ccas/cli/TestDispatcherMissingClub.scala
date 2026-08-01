package ccas.cli

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.utils.errors.ClubProblem

/** Pins `Dispatcher.missingClub`: the "should I bust the completion cache and hint a stale current_club?" test now keys
  * on the typed `problem` field, falling back to the legacy `error` string only for a server that predates it.
  */
object TestDispatcherMissingClub extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("Dispatcher.missingClub")(
    test("typed NotFound counts as missing") {
      assertTrue(Dispatcher.missingClub(Some(ClubProblem.NotFound), None))
    },
    test("typed Problematic counts as missing") {
      assertTrue(Dispatcher.missingClub(Some(ClubProblem.Problematic), None))
    },
    test("legacy 'Club not found' string counts as missing when no typed problem is present") {
      assertTrue(Dispatcher.missingClub(None, Some("Club not found: team-x")))
    },
    test("an unrelated error is not missing") {
      assertTrue(!Dispatcher.missingClub(None, Some("A Membership job is already running")))
    },
    test("no problem and no error is not missing") {
      assertTrue(!Dispatcher.missingClub(None, None))
    }
  )
}
