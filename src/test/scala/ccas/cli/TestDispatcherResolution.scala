package ccas.cli

import zio.{Ref, Task, UIO, ZIO}
import zio.http.Routes
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.ClubResult
import ccas.server.routes.ManagedClubRoutes.ManagedClubResponse

/** Pins how the CLI reads a request's [[ClubResolution]]: which outcomes bust the completion cache and hint a stale
  * `current_club`, which get a note, how an ambiguous name is settled at the prompt, and what `club show` prints.
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

  /** `club show` against a server that answers `resolution` only when asked `asked`: how it ended, and what it printed
    * to stdout and stderr.
    */
  private def show(
    slugs: List[String],
    clubIdOption: Option[Long],
    asked: ClubQuery,
    resolution: ClubResolution
  ): UIO[(Either[Throwable, Int], Vector[String], Vector[String])] =
    for {
      api    <- FakeCcasServer.api(Routes(FakeCcasServer.resolving(asked, resolution, "New Name Club")))
      result <- Dispatcher.showClub(api, slugs, clubIdOption).either
      out    <- TestConsole.output
      err    <- TestConsole.outputErr
    } yield (result, out, err)

  private def listed(clubId: Long, slugOption: Option[String]): ManagedClubResponse =
    ManagedClubResponse(clubId = clubId, slugOption = slugOption, name = s"Club $clubId", markedAt = "t")

  /** What `club list` printed to stdout and stderr for `clubs`, with `current_club` set to `currentClubOption`. */
  private def listing(
    clubs: List[ManagedClubResponse],
    currentClubOption: Option[String]
  ): UIO[(Vector[String], Vector[String])] =
    Dispatcher.printManagedClubs(clubs, currentClubOption) *> TestConsole.output.zip(TestConsole.outputErr)

  private def showing(slug: String, resolution: ClubResolution) =
    show(slugs = List(slug), clubIdOption = None, asked = ClubQuery.BySlug(ClubSlug(slug)), resolution = resolution)

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
      queued <- Ref.make(responses)
      seen   <- Ref.make(List.empty[ClubTarget])
      _      <- ZIO.foreachDiscard(answer)(TestConsole.feedLines(_))
      result <- Dispatcher.sendSettlingAmbiguity(
        target = ClubTarget.byId(ClubId(7)),
        resolutionsOf = (resolution: ClubResolution) => List(resolution),
        interactive = interactive
      )(fakeSubmit(queued, seen))
      targets <- seen.get
    } yield (result, targets)

  override def spec: Spec[Any, Throwable] = suite("TestDispatcherResolution")(
    test("NotLocal, Nameless and RenamedNameless are missing, and answer with what was asked for") {
      assertTrue(
        Dispatcher.missingQuery(ClubResolution.NotLocal(byName)).contains(byName),
        Dispatcher.missingQuery(ClubResolution.Nameless(ClubId(9))).contains(byId),
        Dispatcher.missingQuery(ClubResolution.RenamedNameless(ClubId(3), ClubSlug("x"))).contains(byName)
      )
    },
    test("Known, Renamed and Ambiguous are not missing") {
      assertTrue(
        Dispatcher.missingQuery(ClubResolution.Known(club)).isEmpty,
        Dispatcher.missingQuery(ClubResolution.Renamed(club, ClubSlug("old-name"))).isEmpty,
        Dispatcher.missingQuery(ClubResolution.Ambiguous(ClubSlug("x"), List(clubRef))).isEmpty
      )
    },
    // `club remove` reads its answer this way, since unmanaging needs only the id (#385).
    // A server older than #385 answers a nameless club with no result, so its reason is the one worth showing.
    test("a removal's answer names a club holding no name that the server acted on, and otherwise why it did nothing") {
      val nameless = ClubResolution.Nameless(ClubId(9))
      assertTrue(
        ClubResult("#9", nameless, Some(true)).reachedEither == Right((ClubRef(ClubId(9), None), true)),
        ClubResult("#9", nameless, Some(true)).toEither.isLeft,
        ClubResult("x", ClubResolution.NotLocal(byName), None).reachedEither == Left("Club not found: x"),
        ClubResult("#9", nameless, None).reachedEither.left.exists(_.startsWith("Club #9 can't be reached")),
        ClubResult("new-name", ClubResolution.Known(club), None).reachedEither == Left(
          "the server did not act on new-name"
        )
      )
    },
    // `club remove` unmanages a club holding no name by its id (#385); it was found, so no hint says it is stranded.
    test("a club the request acted on is not missing, so removing it gets no stranded-club note") {
      val nameless = ClubResolution.Nameless(ClubId(9))
      assertTrue(Dispatcher.missingFrom(List(acted(nameless), didNothing(nameless))) == List(nameless))
    },
    test("a current club this server has no record of is noted by how it was asked for, never as renamed") {
      val byIdNote = "note: 'old-slug' is your current club, #9, which this server has no record of. If it was set " +
        "against another server, reach that one with --server; otherwise point at another club with " +
        "'ccas use-club <slug>', or drop it with 'ccas use-club --clear'."
      val bySlugNote =
        "note: 'old-slug' is your current club, but this server knows no club by that name — misspelt, " +
          "or one it hasn't seen yet. Point at another club with 'ccas use-club <slug>', or drop it with " +
          "'ccas use-club --clear'."
      assertTrue(
        Dispatcher.staleCurrentClubHint(Some("9:old-slug"), List(ClubResolution.NotLocal(byId))).contains(byIdNote),
        Dispatcher
          .staleCurrentClubHint(Some("old-slug"), List(ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("old-slug")))))
          .contains(bySlugNote)
      )
    },
    // Repointing by a new name could not help: had the server seen one, the id would have resolved.
    test(
      "a current club the server holds but can't name is noted as waiting for its new name, with no new slug to point at"
    ) {
      val waiting = "which the server holds but can't name now. It will reach the club again once the server sees " +
        "its new name; meanwhile, point at another club with 'ccas use-club <slug>', or drop it with " +
        "'ccas use-club --clear'."
      val nameless        = ClubResolution.Nameless(ClubId(9))
      val renamedNameless = ClubResolution.RenamedNameless(ClubId(3), ClubSlug("old-slug"))
      assertTrue(
        Dispatcher
          .staleCurrentClubHint(Some("9:old-slug"), List(nameless))
          .contains(s"note: 'old-slug' is your current club, #9, $waiting"),
        Dispatcher
          .staleCurrentClubHint(Some("old-slug"), List(renamedNameless))
          .contains(s"note: 'old-slug' is your current club, a former name of #3, $waiting")
      )
    },
    test("a missing club that is not the current one gets no note") {
      val missing = List(ClubResolution.Nameless(ClubId(9)), ClubResolution.NotLocal(byName))
      assertTrue(
        Dispatcher.staleCurrentClubHint(Some("4:elsewhere"), missing).isEmpty,
        Dispatcher.staleCurrentClubHint(None, missing).isEmpty
      )
    },
    test("a former name is noted with the name its club holds now, only for requests that acted") {
      val renamed  = ClubResolution.Renamed(club, ClubSlug("old-name"))
      val outcomes = List(acted(renamed), acted(ClubResolution.Known(club)), didNothing(renamed))
      val note     = "note: 'old-name' is a former name of 'new-name'; using 'new-name'"
      assertTrue(Dispatcher.nameChangesFrom(outcomes) == List(note))
    },
    // `club remove old-name` names the club it unmanaged by id, so the note ties that id to the name typed (#385).
    test("a former name of a club holding none is noted with its id, only for requests that acted") {
      val stranded = ClubResolution.RenamedNameless(ClubId(202), ClubSlug("old-name"))
      val note     = "note: 'old-name' is a former name of club #202, which holds none now"
      assertTrue(Dispatcher.nameChangesFrom(List(acted(stranded), didNothing(stranded))) == List(note))
    },
    test("Moved is not missing — the job ran, just against the club that holds the name now") {
      val moved = ClubResolution.Moved(club, ClubSlug("was-shared"), List(otherRef))
      assertTrue(Dispatcher.missingQuery(moved).isEmpty)
    },
    test("a moved name is noted with its new holder and the clubs it was taken from, only for requests that acted") {
      val moved    = ClubResolution.Moved(club, ClubSlug("was-shared"), List(otherRef))
      val outcomes = List(acted(moved), didNothing(moved), acted(ClubResolution.Known(club)))
      val note = "note: 'was-shared' now belongs to club #1 (now new-name) on Chess.com, not to #2 (now other-name); " +
        "using the club that holds it"
      assertTrue(Dispatcher.nameChangesFrom(outcomes) == List(note))
    },
    // #274: a club holding no name has no name to be noted by, so the note must not run its own words together.
    test("a name moved from a club holding none is noted naming that club by id") {
      val moved = ClubResolution.Moved(club, ClubSlug("went-stale"), List(ClubRef(ClubId(3), None)))
      val note = "note: 'went-stale' now belongs to club #1 (now new-name) on Chess.com, not to #3 (no known name); " +
        "using the club that holds it"
      assertTrue(Dispatcher.nameChangeNote(moved).contains(note))
    },
    // `club show` (#271): the club first, on a line of its own, then why it is that club when the name was not its own.
    suite("club show")(
      test("prints the club a name reaches on one line") {
        showing(slug = "new-name", resolution = ClubResolution.Known(club)).map { (result, out, err) =>
          assertTrue(result == Right(0), out == Vector("#1  new-name  New Name Club\n"), err.isEmpty)
        }
      },
      test("--club-id asks by id") {
        show(
          slugs = Nil,
          clubIdOption = Some(1L),
          asked = ClubQuery.ById(ClubId(1)),
          resolution = ClubResolution.Known(club)
        ).map((result, out, _) => assertTrue(result == Right(0), out == Vector("#1  new-name  New Name Club\n")))
      },
      test("says when the name is a former one of the club") {
        showing(slug = "old-name", resolution = ClubResolution.Renamed(club, ClubSlug("old-name"))).map { (_, out, _) =>
          assertTrue(out == Vector("#1  new-name  New Name Club\n", "'old-name' is a former name of this club\n"))
        }
      },
      test("says whose name a moved one was") {
        showing(slug = "was-shared", resolution = ClubResolution.Moved(club, ClubSlug("was-shared"), List(otherRef)))
          .map { (_, out, _) =>
            assertTrue(
              out == Vector(
                "#1  new-name  New Name Club\n",
                "'was-shared' now belongs to this club on Chess.com, not to #2 (now other-name)\n"
              )
            )
          }
      },
      test("a name that reaches no club fails with the server's reason, and prints no club") {
        showing(slug = "x", resolution = ClubResolution.NotLocal(byName)).map { (result, out, _) =>
          assertTrue(result == Left(CliError("Club not found: x", 1)), out.isEmpty)
        }
      },
      // A look acts on nothing, so there is nothing to pick a club for: the holders are the answer.
      test("an ambiguous name fails listing its holders, and prompts for nothing") {
        showing(slug = "was-shared", resolution = contested).map { (result, out, err) =>
          assertTrue(
            result.left.exists(_.getMessage.contains("#1 (now new-name), #2 (now other-name)")),
            out.isEmpty,
            err.isEmpty
          )
        }
      }
    ),
    // `club list` (#385): every managed club, a club holding no name by its id, with `*` on the club the pointer means.
    suite("club list")(
      test("shows a managed club that holds no name by its id") {
        listing(clubs = List(listed(1, Some("new-name")), listed(4123, None)), currentClubOption = None).map {
          (out, err) =>
            assertTrue(
              out == Vector("  new-name  Club 1  marked=t\n", "  #4123 (no known name)  Club 4123  marked=t\n"),
              err.isEmpty
            )
        }
      },
      // The name an id pointer shows can since have passed to another club, which must not be marked as current.
      test("marks the club an id pointer means, not another club holding the name it shows") {
        listing(List(listed(1, Some("new-name")), listed(4123, None)), Some("4123:new-name")).map { (out, err) =>
          assertTrue(
            out == Vector("  new-name  Club 1  marked=t\n", "* #4123 (no known name)  Club 4123  marked=t\n"),
            err.isEmpty
          )
        }
      },
      test("does not call an id pointer's nameless club unmanaged") {
        listing(List(listed(4123, None)), Some("4123:old-name")).map((_, err) => assertTrue(err.isEmpty))
      },
      // `club add old-name` would add whichever club holds that name now, which may not be the pointer's.
      test("tells an id pointer whose club is unmanaged to add it by id") {
        listing(List(listed(1, Some("new-name"))), Some("4123:old-name")).map { (_, err) =>
          assertTrue(err.exists(_.contains("add it with 'ccas club add --club-id 4123'")))
        }
      }
    ),
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
    // A candidate holding no name resolves `Nameless` by id, so offering it would offer a command that fails.
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
        .map { (result, targets) =>
          assertTrue(
            result == ClubResolution.Known(other),
            targets.map(_.query) == List(ClubQuery.ById(ClubId(7)), ClubQuery.ById(ClubId(2)))
          )
        }
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
