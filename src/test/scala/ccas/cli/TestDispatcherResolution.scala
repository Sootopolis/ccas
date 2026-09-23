package ccas.cli

import zio.{Ref, Task, ZIO}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}

/** Pins how the CLI reads a request's [[ClubResolution]]: which outcomes bust the completion cache and hint a stale
  * `current_club`, which get a note, and how an ambiguous name is settled at the prompt.
  */
object TestDispatcherResolution extends ZIOSpecDefault {

  private val club     = NamedClub(ClubId(1), ClubSlug("new-name"))
  private val other    = NamedClub(ClubId(2), ClubSlug("other-name"))
  private val clubRef  = ClubRef.of(club)
  private val otherRef = ClubRef.of(other)

  private val byName: ClubQuery = ClubQuery.BySlug(ClubSlug("x"))
  private val byId: ClubQuery   = ClubQuery.ById(ClubId(9))

  private val contested: ClubResolution.Ambiguous =
    ClubResolution.Ambiguous(ClubSlug("was-shared"), List(clubRef, otherRef))

  private def acted(resolution: ClubResolution)      = Dispatcher.ClubOutcome(resolution = resolution, acted = true)
  private def didNothing(resolution: ClubResolution) = Dispatcher.ClubOutcome(resolution = resolution, acted = false)

  private def answering(input: String) =
    TestConsole.feedLines(input) *> Dispatcher.promptForClub(contested, interactive = true)

  /** A fake submit, which is all the resend loop needs: it answers with each queued resolution in turn (repeating the
    * last) and records the target it was asked about, so a test can see whether a second send happened and for whom.
    */
  private def fakeSubmit(
    queued: Ref[List[ClubResolution]],
    seen: Ref[List[ClubTarget]]
  ): ClubTarget => Task[ClubResolution] =
    target =>
      seen.update(_ :+ target) *> queued.modify {
        case head :: Nil  => (head, List(head))
        case head :: tail => (head, tail)
        case Nil          => (ClubResolution.NotLocal(byName), Nil)
      }

  private def settling(
    answer: Option[String],
    responses: List[ClubResolution],
    interactive: Boolean
  ): Task[(ClubResolution, List[ClubTarget])] =
    for {
      queued  <- Ref.make(responses)
      seen    <- Ref.make(List.empty[ClubTarget])
      _       <- ZIO.foreachDiscard(answer)(TestConsole.feedLines(_))
      result  <- Dispatcher.sendSettlingAmbiguity(
        target = ClubTarget.byId(ClubId(7)),
        resolutionsOf = (resolution: ClubResolution) => List(resolution),
        interactive = interactive
      )(fakeSubmit(queued, seen))
      targets <- seen.get
    } yield (result, targets)

  override def spec: Spec[Any, Throwable] = suite("TestDispatcherResolution")(
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
        Dispatcher.missingQuery(ClubResolution.Ambiguous(ClubSlug("x"), List(clubRef))).isEmpty
      )
    },
    test("renamedFrom pairs the requested slug with the current one, only for Renamed requests that acted") {
      val renamed  = ClubResolution.Renamed(club, ClubSlug("old-name"))
      val outcomes = List(acted(renamed), acted(ClubResolution.Known(club)), didNothing(renamed))
      assertTrue(Dispatcher.renamedFrom(outcomes) == List(Dispatcher.RenamedName("old-name", "new-name")))
    },
    test("Moved is not missing — the job ran, just against the club that holds the name now") {
      val moved = ClubResolution.Moved(club, ClubSlug("was-shared"), List(otherRef))
      assertTrue(Dispatcher.missingQuery(moved).isEmpty)
    },
    test("movedFrom names the new holder and the clubs it was taken from, only for requests that acted") {
      val moved    = ClubResolution.Moved(club, ClubSlug("was-shared"), List(otherRef))
      val outcomes = List(acted(moved), didNothing(moved), acted(ClubResolution.Known(club)))
      val moves = List(Dispatcher.MovedName(requested = "was-shared", holder = club, previous = List(otherRef)))
      assertTrue(Dispatcher.movedFrom(outcomes) == moves)
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
    // A candidate holding no name resolves `Problematic` by id, so offering it would offer a command that fails.
    test("a candidate holding no name is not offered, and the numbering skips it") {
      val nameless = ClubRef(ClubId(3), None)
      val mixed: ClubResolution.Ambiguous =
        ClubResolution.Ambiguous(ClubSlug("was-shared"), List(nameless, clubRef))
      for {
        picked  <- TestConsole.feedLines("1") *> Dispatcher.promptForClub(mixed, interactive = true)
        offered <- TestConsole.outputErr
      } yield assertTrue(
        picked.contains(ClubId(1)),
        offered.exists(_.contains("1) #1 (now new-name)")),
        offered.exists(_.contains("1 more hold no name we know of")),
        !offered.exists(_.contains("2)"))
      )
    },
    test("an ambiguity whose candidates all hold no name is not prompted at all") {
      val allGone: ClubResolution.Ambiguous =
        ClubResolution.Ambiguous(ClubSlug("was-shared"), List(ClubRef(ClubId(3), None)))
      for {
        picked <- TestConsole.feedLines("1") *> Dispatcher.promptForClub(allGone, interactive = true)
        unread <- TestConsole.outputErr
      } yield assertTrue(picked.isEmpty, unread.isEmpty)
    },
    // The resend loop itself: what the prompt's answer is actually used for (#254).
    test("an ambiguous answer is settled by resending for the club picked at the prompt") {
      settling(answer = Some("2"), responses = List(contested, ClubResolution.Known(other)), interactive = true)
        .map((result, targets) =>
          assertTrue(
            result == ClubResolution.Known(other),
            targets.map(_.query) == List(ClubQuery.ById(ClubId(7)), ClubQuery.ById(ClubId(2)))
          )
        )
    },
    test("an aborted prompt keeps the ambiguous answer, and sends nothing a second time") {
      settling(answer = Some(""), responses = List(contested, ClubResolution.Known(other)), interactive = true)
        .map((result, targets) => assertTrue(result == contested, targets.size == 1))
    },
    test("a headless run keeps the ambiguous answer rather than resending for a club nobody picked") {
      settling(answer = None, responses = List(contested, ClubResolution.Known(other)), interactive = false)
        .map((result, targets) => assertTrue(result == contested, targets.size == 1))
    },
    test("an answer that is not ambiguous is returned as it came, with one send") {
      settling(answer = None, responses = List(ClubResolution.Known(club)), interactive = true)
        .map((result, targets) => assertTrue(result == ClubResolution.Known(club), targets.size == 1))
    },
    test("nothing is prompted without a terminal — the error names --club-id instead") {
      for {
        picked <- TestConsole.feedLines("1") *> Dispatcher.promptForClub(contested, interactive = false)
        unread <- TestConsole.outputErr
      } yield assertTrue(picked.isEmpty, unread.isEmpty)
    }
  )
}
