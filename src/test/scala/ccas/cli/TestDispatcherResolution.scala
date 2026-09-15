package ccas.cli

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.{ClubRef, ClubResolution}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.JobRoutes.ClubJobResult

/** Pins how the CLI reads a submit's [[ClubResolution]]: which outcomes bust the completion cache and hint a stale
  * `current_club`, and which get a "former name" note.
  */
object TestDispatcherResolution extends ZIOSpecDefault {

  private val club = ClubRef(ClubId(1), ClubSlug("new-name"))

  override def spec: Spec[Any, Nothing] = suite("TestDispatcherResolution")(
    test("NotLocal counts as missing") {
      assertTrue(Dispatcher.missingClub(ClubResolution.NotLocal(ClubSlug("x"))))
    },
    test("Problematic counts as missing") {
      assertTrue(Dispatcher.missingClub(ClubResolution.Problematic(ClubSlug("x"))))
    },
    test("Known, Renamed and Ambiguous are not missing") {
      assertTrue(
        !Dispatcher.missingClub(ClubResolution.Known(club)),
        !Dispatcher.missingClub(ClubResolution.Renamed(club, ClubSlug("old-name"))),
        !Dispatcher.missingClub(ClubResolution.Ambiguous(ClubSlug("x"), List(club)))
      )
    },
    test("renamedFrom pairs the requested slug with the current one, only for Renamed results that started a job") {
      val renamedResolution = ClubResolution.Renamed(club, ClubSlug("old-name"))
      val renamed           = ClubJobResult("old-name", Some("j1"), None, renamedResolution)
      val known             = ClubJobResult("new-name", Some("j2"), None, ClubResolution.Known(club))
      val conflict          = ClubJobResult("old-name", None, Some("already running"), renamedResolution)
      assertTrue(Dispatcher.renamedFrom(List(renamed, known, conflict)) == List("old-name" -> "new-name"))
    }
  )
}
