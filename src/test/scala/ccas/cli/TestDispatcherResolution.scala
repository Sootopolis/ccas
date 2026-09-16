package ccas.cli

import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.JobRoutes.ClubJobResult

/** Pins how the CLI reads a submit's [[ClubResolution]]: which outcomes bust the completion cache and hint a stale
  * `current_club`, which get a note, and how an ambiguous name is settled at the prompt.
  */
object TestDispatcherResolution extends ZIOSpecDefault {

  private val club  = ClubRef(ClubId(1), ClubSlug("new-name"))
  private val other = ClubRef(ClubId(2), ClubSlug("other-name"))

  private val byName: ClubQuery = ClubQuery.BySlug(ClubSlug("x"))
  private val byId: ClubQuery   = ClubQuery.ById(ClubId(9))

  private val contested: ClubResolution.Ambiguous =
    ClubResolution.Ambiguous(ClubSlug("was-shared"), List(club, other))

  private def answering(input: String) =
    TestConsole.feedLines(input) *> Dispatcher.promptForClub(contested, interactive = true)

  override def spec: Spec[Any, Nothing] = suite("TestDispatcherResolution")(
    test("NotLocal and Problematic are missing, and answer with what was asked for") {
      assertTrue(
        Dispatcher.missingQuery(ClubResolution.NotLocal(byName)).contains(byName),
        Dispatcher.missingQuery(ClubResolution.Problematic(byId)).contains(byId)
      )
    },
    test("Known, Renamed and Ambiguous are not missing") {
      assertTrue(
        Dispatcher.missingQuery(ClubResolution.Known(club)).isEmpty,
        Dispatcher.missingQuery(ClubResolution.Renamed(club, ClubSlug("old-name"))).isEmpty,
        Dispatcher.missingQuery(ClubResolution.Ambiguous(ClubSlug("x"), List(club))).isEmpty
      )
    },
    test("renamedFrom pairs the requested slug with the current one, only for Renamed results that started a job") {
      val renamedResolution = ClubResolution.Renamed(club, ClubSlug("old-name"))
      val renamed           = ClubJobResult("old-name", Some("j1"), None, renamedResolution)
      val known             = ClubJobResult("new-name", Some("j2"), None, ClubResolution.Known(club))
      val conflict          = ClubJobResult("old-name", None, Some("already running"), renamedResolution)
      assertTrue(Dispatcher.renamedFrom(List(renamed, known, conflict)) == List("old-name" -> "new-name"))
    },
    test("Moved is not missing — the job ran, just against the club that holds the name now") {
      assertTrue(Dispatcher.missingQuery(ClubResolution.Moved(club, ClubSlug("was-shared"), List(other))).isEmpty)
    },
    test("movedFrom names the new holder and the clubs it was taken from, only for results that started a job") {
      val movedResolution = ClubResolution.Moved(club, ClubSlug("was-shared"), List(other))
      val moved           = ClubJobResult("was-shared", Some("j1"), None, movedResolution)
      val conflict        = ClubJobResult("was-shared", None, Some("already running"), movedResolution)
      val known           = ClubJobResult("new-name", Some("j2"), None, ClubResolution.Known(club))
      assertTrue(Dispatcher.movedFrom(List(moved, conflict, known)) == List(("was-shared", club, List(other))))
    },
    test("the prompt answers with the club at the number picked") {
      answering("2").map(picked => assertTrue(picked.contains(ClubId(2))))
    },
    test("a blank, out-of-range or non-numeric answer aborts rather than guessing at a club") {
      for {
        blank      <- answering("")
        outOfRange <- answering("3")
        zero       <- answering("0")
        text       <- answering("other-name")
      } yield assertTrue(blank.isEmpty, outOfRange.isEmpty, zero.isEmpty, text.isEmpty)
    },
    test("nothing is prompted without a terminal — the submit error names --club-id instead") {
      for {
        picked <- TestConsole.feedLines("1") *> Dispatcher.promptForClub(contested, interactive = false)
        unread <- TestConsole.outputErr
      } yield assertTrue(picked.isEmpty, unread.isEmpty)
    }
  )
}
