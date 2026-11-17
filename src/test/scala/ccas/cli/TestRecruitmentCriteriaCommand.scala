package ccas.cli

import java.nio.file.Files
import java.time.Instant

import zio.{Ref, Task, UIO, ZIO}
import zio.http.{handler, string, Method, Request, Response, Routes, Status}
import zio.json.{DecoderOps, EncoderOps, JsonCodec}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution, NamedClub}
import ccas.analysis.apps.recruitment.CriteriaSpec
import ccas.analysis.tables.RecruitmentCriteria
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Elo}
import ccas.server.routes.{ClubRequest, ClubResult}
import ccas.server.routes.ClubRoutes.ClubInfo
import ccas.server.routes.RecruitmentCriteriaRoutes.{
  AliasSummary,
  CriteriaView,
  SetCriteriaRequest,
  SetCriteriaResponse
}

/** `ccas recruitment-criteria show | set | list` (#485), through [[CriteriaCommand]] against a [[FakeCcasServer]] that
  * stores one club's criteria by name, as sent. Its recap names a version by id and shows two settings, enough to
  * tell which version, and which values, a command printed.
  */
object TestRecruitmentCriteriaCommand extends ZIOSpecDefault {

  private val alpha = NamedClub(ClubId(1), ClubSlug("team-alpha"))
  private val beta  = NamedClub(ClubId(2), ClubSlug("team-beta"))
  private val since = Instant.parse("2026-03-18T14:02:00Z")
  private val stored: CriteriaSpec =
    CriteriaSpec.fromCriteria(
      RecruitmentCriteria.defaultDaily.copy(maxClubs = Some(40), daysSinceLastInvited = Some(90))
    )

  private final case class Version(id: Long, spec: CriteriaSpec, since: Instant)

  private final case class Fake(
    criteria: Ref[Map[String, Version]],
    posts: Ref[List[SetCriteriaRequest]],
    requests: Ref[Int],
    lookups: Ref[Int],
    interloper: Boolean
  ) {
    def routes: Routes[Any, Response] = Routes(
      Method.GET / "api" / "recruitment-criteria" -> handler { (_: Request) =>
        for {
          _     <- requests.update(_ + 1)
          names <- criteria.get
        } yield answer(names.toList.sortBy(_._1).map((name, v) => AliasSummary(name, v.since.toString, v.id)))
      },
      Method.GET / "api" / "recruitment-criteria" / string("name") -> handler { (name: String, _: Request) =>
        for {
          _     <- requests.update(_ + 1)
          names <- criteria.get
        } yield names.get(name).fold(Response.status(Status.NotFound)) { v =>
          answer(CriteriaView(Some(v.id), v.spec, recap(name, v)))
        }
      },
      Method.GET / "api" / "clubs" / "resolve" -> handler { (req: Request) =>
        val queryEither = ClubRequest.fromParams(req.queryParams("clubId"), req.queryParams("slug"))
        (requests.update(_ + 1) *> lookups.update(_ + 1)).as(
          queryEither.fold(_ => Response.status(Status.BadRequest), resolved)
        )
      },
      Method.POST / "api" / "recruitment-criteria" -> handler { (req: Request) =>
        for {
          _    <- requests.update(_ + 1)
          body <- req.body.asString.orElseSucceed("")
          set  <- ZIO.fromEither(body.fromJson[SetCriteriaRequest]).orElseFail(Response.status(Status.BadRequest))
          _    <- posts.update(_ :+ set)
          id <- criteria.modify { names =>
            val next = names.values.map(_.id).maxOption.getOrElse(0L) + 1
            // Another writer's save lands straight after this one, so the name's latest version is not this one.
            val latest = if (interloper) { next + 1 }
            else { next }
            next -> names.updated(set.alias, Version(latest, set.criteria, since.plusSeconds(latest)))
          }
        } yield answer(SetCriteriaResponse(id))
      }
    )
  }

  // `team-alpha` and `team-beta` are known; `old-name` reaches a club that has since lost it, `moved-name` has passed
  // to team-beta, `shared` was held by two, and looking up `flaky`, `9999` or club #500 fails. Anything else is unknown.
  private def resolved(query: ClubQuery): Response = {
    def unnamed(resolution: ClubResolution) =
      FakeCcasServer.json(ClubResult[ClubInfo](query.describe, resolution, None))
    query match {
      case ClubQuery.ById(id) if id == ClubId(500) => Response.status(Status.InternalServerError)
      case ClubQuery.BySlug(slug) if Set("flaky", "9999").contains(ClubSlug.unwrap(slug)) =>
        Response.status(Status.InternalServerError)
      case ClubQuery.BySlug(slug) if slug == ClubSlug("moved-name") =>
        unnamed(
          ClubResolution.Moved(beta, ClubSlug("moved-name"), List(ClubRef(ClubId(6), Some(ClubSlug("old-holder")))))
        )
      case ClubQuery.BySlug(slug) if slug == ClubSlug("old-name") =>
        unnamed(ClubResolution.RenamedNameless(ClubId(5), ClubSlug("old-name")))
      case ClubQuery.BySlug(slug) if slug == ClubSlug("shared") =>
        unnamed(
          ClubResolution.Ambiguous(
            ClubSlug("shared"),
            List(ClubRef(ClubId(3), Some(ClubSlug("now-c"))), ClubRef(ClubId(4), None))
          )
        )
      case _ =>
        val knownOption = List(alpha, beta).find { club =>
          query match {
            case ClubQuery.ById(id)     => club.clubId == id
            case ClubQuery.BySlug(slug) => club.slug == slug
          }
        }
        knownOption.fold(unnamed(ClubResolution.NotLocal(query))) { club =>
          FakeCcasServer.json(
            ClubResult(ClubSlug.unwrap(club.slug), ClubResolution.Known(club), Some(ClubInfo(club.slug, "Club")))
          )
        }
    }
  }

  private def answer[A: JsonCodec](value: A): Response =
    FakeCcasServer.json(ClubResult(ClubQuery.BySlug(alpha.slug).describe, ClubResolution.Known(alpha), Some(value)))

  private def recap(name: String, v: Version): List[String] =
    List(
      s"team-alpha's criteria \"$name\", version ${v.id}:",
      s"  maxClubs              ${v.spec.maxClubs.getOrElse("-")}",
      s"  daysSinceLastInvited  ${v.spec.daysSinceLastInvited.getOrElse("-")}"
    )

  private def fake(names: Map[String, Version]): UIO[Fake] = fakeWith(names, interloper = false)

  private def fakeWith(names: Map[String, Version], interloper: Boolean): UIO[Fake] =
    for {
      criteria <- Ref.make(names)
      posts    <- Ref.make(List.empty[SetCriteriaRequest])
      requests <- Ref.make(0)
      lookups  <- Ref.make(0)
    } yield Fake(criteria, posts, requests, lookups, interloper)

  private val withDefault = Map("default" -> Version(1, stored, since))

  private def show(f: Fake, json: Boolean): Task[Int] =
    for {
      wiring <- TempWiring(f.routes)
      api    <- FakeCcasServer.api(f.routes)
      code <- CriteriaCommand.show(
        wiring,
        api,
        None,
        CliCommand.CriteriaShow(FakeCcasServer.Url, Some("team-alpha"), None, json, Nil)
      )
    } yield code

  private def set(f: Fake, names: List[String], fileOption: Option[String], interactive: Boolean): Task[Int] =
    for {
      wiring <- TempWiring(f.routes)
      api    <- FakeCcasServer.api(f.routes)
      command = CliCommand.CriteriaSet(FakeCcasServer.Url, Some("team-alpha"), None, fileOption, names)
      code <- CriteriaCommand.set(wiring, api, None, command, interactive)
    } yield code

  // Through `dispatch`, as `Main` runs it, with `defaultServer` as the one a command without `--server` reaches.
  private def list(f: Fake, defaultServer: String): Task[Int] =
    for {
      wiring <- TempWiring(f.routes)
      command = CliCommand.CriteriaList(FakeCcasServer.Url, Some("team-alpha"), None)
      code <- Dispatcher.dispatch(wiring)(command, None, defaultServer)
    } yield code.code

  // One answer per setting, in prompt order: Enter keeps each, except those given by position.
  private val SettingCount = 22
  private def answers(changed: (Int, String)*): List[String] =
    changed.foldLeft(List.fill(SettingCount)("")) { case (lines, (index, value)) => lines.updated(index, value) }
  private val MaxClubs             = 6
  private val DaysSinceLastInvited = 1
  private val NationalityExclude   = 3
  private val DailyMinElo          = 10
  private val DailyMaxElo          = 11
  private val ExcludeClubs         = 5
  private val DailyMinScoreRate    = 12
  private val DailyMaxScoreRate    = 13
  private val DailyMinOngoingGames = 19
  private val DailyMaxOngoingGames = 20

  // [[answers]], with the setting at `index` answered `refused` and then, asked again, `accepted`.
  private def answersAskedAgain(
    index: Int,
    refused: String,
    accepted: String,
    changed: (Int, String)*
  ): List[String] = {
    val lines = answers(changed*)
    lines.take(index) ++ List(refused, accepted) ++ lines.drop(index + 1)
  }

  // `json` as a criteria file for `use`, deleted afterwards.
  private def withCriteriaFile[A](json: String)(use: String => Task[A]): Task[A] =
    ZIO.acquireReleaseWith(ZIO.attemptBlocking(Files.writeString(Files.createTempFile("criteria", ".json"), json)))(
      file => ZIO.attemptBlocking(Files.deleteIfExists(file)).ignore
    )(file => use(file.toString))

  private def failure(exit: zio.Exit[Throwable, Int]): Option[CliError] =
    exit.causeOption.flatMap(_.failureOption).collect { case e: CliError => e }

  private def output: UIO[String] = TestConsole.output.map(_.mkString)

  private def errors: UIO[String] = TestConsole.outputErr.map(_.mkString)

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentCriteriaCommand")(
    test("show prints the recap of the criteria named") {
      for {
        f    <- fake(withDefault)
        code <- show(f, json = false)
        out  <- output
      } yield assertTrue(code == 0, out.contains(recap("default", withDefault("default")).mkString("\n")))
    },
    test("show --json prints criteria a set --file can read back") {
      for {
        f     <- fake(withDefault)
        _     <- show(f, json = true)
        out   <- output
        code  <- withCriteriaFile(out)(file => set(f, Nil, Some(file), interactive = false))
        posts <- f.posts.get
        after <- output
      } yield assertTrue(
        out.fromJson[CriteriaSpec] == Right(stored),
        code == 0,
        posts.isEmpty,
        after.contains("No changes; nothing saved.")
      )
    },
    test("set --file on a terminal shows the changes and asks before saving") {
      val changed = stored.copy(maxClubs = Some(25))
      for {
        _     <- TestConsole.feedLines("n")
        f     <- fake(withDefault)
        code  <- withCriteriaFile(changed.toJson)(file => set(f, Nil, Some(file), interactive = true))
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        code == 0,
        posts.isEmpty,
        out.contains("  maxClubs: 40 → 25"),
        out.contains("scheduled runs included.\nSave? [Y/n]"),
        out.contains("Not saved.")
      )
    },
    test("set --file with no terminal saves the changes without asking") {
      val changed = stored.copy(maxClubs = Some(25))
      for {
        f     <- fake(withDefault)
        code  <- withCriteriaFile(changed.toJson)(file => set(f, Nil, Some(file), interactive = false))
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        code == 0,
        out.contains("  maxClubs: 40 → 25"),
        !out.contains("Save?"),
        out.contains("Saved."),
        posts.map(p => (p.club, p.alias, p.criteria)) == List((ClubQuery.ById(alpha.clubId), "default", changed))
      )
    },
    test("list names each criteria with its version") {
      for {
        f   <- fake(withDefault + ("weekend-long" -> Version(2, stored, Instant.parse("2026-11-01T09:30:00Z"))))
        _   <- list(f, FakeCcasServer.Url)
        out <- output
      } yield assertTrue(
        out.contains("team-alpha's criteria:"),
        out.contains("  \"default\"       version of 2026-03-18 14:02 UTC"),
        out.contains("  \"weekend-long\"  version of 2026-11-01 09:30 UTC")
      )
    },
    test("list for a club with no criteria says how to set some") {
      for {
        f   <- fake(Map.empty)
        _   <- list(f, FakeCcasServer.Url)
        out <- output
      } yield assertTrue(
        out.contains(
          "team-alpha has no criteria yet. Set some with: ccas recruitment-criteria set --club team-alpha default"
        )
      )
    },
    test("list against a server other than the default names it in the command it suggests") {
      for {
        f   <- fake(Map.empty)
        _   <- list(f, CliCommand.DefaultServer)
        out <- output
      } yield assertTrue(
        out.contains(s"ccas recruitment-criteria set --server ${FakeCcasServer.Url} --club team-alpha default")
      )
    },
    test("set starts from the current values and saves only what changed") {
      for {
        _     <- TestConsole.feedLines((answers(MaxClubs -> "25") :+ "y")*)
        f     <- fake(withDefault)
        code  <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        code == 0,
        out.contains("Editing team-alpha's criteria \"default\" (Enter keeps the current value)."),
        out.contains("maxClubs [40]"),
        out.contains("  maxClubs: 40 → 25"),
        posts.map(p => (p.club, p.alias, p.criteria)) ==
          List((ClubQuery.ById(alpha.clubId), "default", stored.copy(maxClubs = Some(25))))
      )
    },
    test("set for a new name starts from the defaults") {
      for {
        _     <- TestConsole.feedLines((answers() :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, List("weekend"), None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("New criteria \"weekend\" for team-alpha, starting from the defaults (Enter keeps)."),
        out.contains("--- Changes ---\n  (new criteria, the same as the defaults)\n"),
        posts.map(p => (p.alias, p.criteria)) ==
          List(("weekend", CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily)))
      )
    },
    test("a new name's changes are shown against the defaults before it is saved") {
      for {
        _   <- TestConsole.feedLines((answers(MaxClubs -> "25") :+ "y")*)
        f   <- fake(withDefault)
        _   <- set(f, List("weekend"), None, interactive = true)
        out <- output
      } yield assertTrue(
        out.contains(
          "--- Changes ---\n  (new criteria; what differs from the defaults)\n  maxClubs: 40 → 25\n" +
            "Saving makes a new version"
        )
      )
    },
    test("a flag is offered as yes or no, and an answer that is neither is asked again") {
      val lines = answers()
      for {
        _ <- TestConsole.feedLines(
          (lines.take(NationalityExclude) ++ List("yep", "y") ++ lines.drop(NationalityExclude + 1) :+ "y")*
        )
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("nationalityExclude [no] (y/n, Enter=keep):   enter y or n\nnationalityExclude [no]"),
        posts.map(_.criteria.nationalityExclude) == List(true)
      )
    },
    test("a value out of range is asked again at once, and the other answers are kept") {
      val lines = answersAskedAgain(DailyMinScoreRate, "55", "0.55", MaxClubs -> "25")
      for {
        _     <- TestConsole.feedLines((lines :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("  dailyMinScoreRate must be in [0, 1] (got 55.0)\ndailyMinScoreRate ["),
        posts.map(_.criteria) == List(stored.copy(maxClubs = Some(25), dailyMinScoreRate = Some(0.55)))
      )
    },
    test("a negative count is asked again") {
      for {
        _     <- TestConsole.feedLines((answersAskedAgain(MaxClubs, "-1", "25") :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("  maxClubs must be >= 0 (got -1)\nmaxClubs ["),
        posts.map(_.criteria.maxClubs) == List(Some(25))
      )
    },
    test("a max below its min is asked again at the max, a kept one included") {
      val lines = answersAskedAgain(DailyMaxScoreRate, "", "-", DailyMinScoreRate -> "0.9")
      for {
        _     <- TestConsole.feedLines((lines :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("  dailyScoreRate min (0.9) must be <= max (0.875)\ndailyMaxScoreRate ["),
        posts.map(_.criteria) == List(stored.copy(dailyMinScoreRate = Some(0.9), dailyMaxScoreRate = None))
      )
    },
    test("an Elo max typed below its min is asked again at the max") {
      val lines = answersAskedAgain(DailyMaxElo, "1500", "2200", DailyMinElo -> "2000")
      for {
        _     <- TestConsole.feedLines((lines :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("  dailyElo min (2000) must be <= max (1500)\ndailyMaxElo ["),
        posts.map(p => (p.criteria.dailyMinElo, p.criteria.dailyMaxElo)) ==
          List((Some(Elo(2000.toShort)), Some(Elo(2200.toShort))))
      )
    },
    test("a setting that breaks two rules is told both") {
      val lines = answersAskedAgain(DailyMaxOngoingGames, "-1", "", DailyMinOngoingGames -> "10")
      for {
        _     <- TestConsole.feedLines((lines :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains(
          "  dailyMaxOngoingGames must be >= 0 (got -1); dailyOngoingGames min (10) must be <= max (-1)\n" +
            "dailyMaxOngoingGames ["
        ),
        posts.map(_.criteria) == List(stored.copy(dailyMinOngoingGames = Some(10)))
      )
    },
    test("each answer is saved to the setting it was asked for") {
      val typed = List(
        "100",
        "101",
        "102",
        "y",
        "GB,FR",
        "team-beta",
        "106",
        "n",
        "108",
        "n",
        "1010",
        "1011",
        "0.12",
        "0.13",
        "114",
        "115",
        "16",
        "17",
        "118",
        "119",
        "120",
        "121"
      )
      val expected = RecruitmentCriteria(
        criteriaId = 0,
        minDaysSinceRegistration = Some(100),
        daysSinceLastInvited = Some(101),
        daysSinceRejected = Some(102),
        nationalityExclude = true,
        nationalityCountries = List("GB", "FR"),
        excludeClubs = List(beta.clubId),
        maxClubs = Some(106),
        excludeSourceAdmins = false,
        avoidAdminMinClubSize = Some(108),
        excludeFormerMembers = false,
        dailyMinElo = Some(Elo(1010.toShort)),
        dailyMaxElo = Some(Elo(1011.toShort)),
        dailyMinScoreRate = Some(0.12),
        dailyMaxScoreRate = Some(0.13),
        dailyMinGamesFinished = Some(114),
        dailyMinTmGamesFinished = Some(115),
        dailyMaxTimeoutPercent = Some(16.0),
        dailyMaxTmTimeoutPercent = Some(17.0),
        dailyMaxHoursPerMove = Some(118),
        dailyMinOngoingGames = Some(119),
        dailyMaxOngoingGames = Some(120),
        dailyMinOngoingTeamMatches = Some(121)
      )
      for {
        _     <- TestConsole.feedLines((typed :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
      } yield assertTrue(typed.size == SettingCount, posts.map(_.criteria) == List(CriteriaSpec.fromCriteria(expected)))
    },
    // A rule is checked at the prompt whose name is its setting, so this is what puts every rule before the operator.
    test("each setting is asked under its own name, in the order the criteria hold them") {
      for {
        _   <- TestConsole.feedLines(answers()*)
        f   <- fake(withDefault)
        _   <- set(f, Nil, None, interactive = true)
        out <- output
      } yield {
        val asked    = """(\w+) \[[^\]]*\] \(""".r.findAllMatchIn(out).map(_.group(1)).toList
        val settings = RecruitmentCriteria.defaultDaily.productElementNames.filterNot(_ == "criteriaId").toList
        assertTrue(asked == settings)
      }
    },
    // `(Enter=keep, - =none)` is the prompt that checks the rules; a flag, list or clubs prompt doesn't yet.
    test("every rule's setting is asked by a prompt that checks the rules") {
      for {
        _   <- TestConsole.feedLines(answers()*)
        f   <- fake(withDefault)
        _   <- set(f, Nil, None, interactive = true)
        out <- output
      } yield {
        val checking = """(\w+) \[[^\]]*\] \(Enter=keep, - =none\)""".r.findAllMatchIn(out).map(_.group(1)).toSet
        assertTrue(RecruitmentCriteria.Rules.map(_.setting).toSet.subsetOf(checking))
      }
    },
    test("excluded clubs are shown and taken as slugs, named in the preview, and saved as ids") {
      val stays     = List(beta.clubId, ClubId(99), ClubId(500))
      val excluding = Map("default" -> Version(1, stored.copy(excludeClubs = stays), since))
      for {
        _       <- TestConsole.feedLines((answers(ExcludeClubs -> "team-alpha, #7") :+ "y")*)
        f       <- fake(excluding)
        _       <- set(f, Nil, None, interactive = true)
        posts   <- f.posts.get
        out     <- output
        lookups <- f.lookups.get
      } yield assertTrue(
        out.contains("excludeClubs [team-beta,#99,#500] (comma-separated slugs or #ids"),
        out.contains("  excludeClubs: [team-beta,#99,#500] → [team-alpha,#7]\n"),
        // Three clubs shown, one slug typed, and #7, the one club the prompt never named, for the preview.
        lookups == 5,
        posts.map(_.criteria.excludeClubs) == List(List(alpha.clubId, ClubId(7)))
      )
    },
    test("a name a club has since lost excludes that club, and a name several held asks for its #<id>") {
      for {
        _     <- TestConsole.feedLines((answersAskedAgain(ExcludeClubs, "shared", "old-name") :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
        err   <- errors
      } yield assertTrue(
        out.contains(
          "  no club holds shared now, and it was held by #3 (now now-c), #4 (no known name); give the one you mean " +
            "as #<id>\nexcludeClubs ["
        ),
        err.contains("note: 'old-name' is a former name of club #5, which holds none now\n"),
        posts.map(_.criteria.excludeClubs) == List(List(ClubId(5)))
      )
    },
    test("a name that has passed to another club is noted as club commands note it, and the club cache cleared") {
      val command = CliCommand.CriteriaSet(FakeCcasServer.Url, Some("team-alpha"), None, None, Nil)
      for {
        _      <- TestConsole.feedLines((answers(ExcludeClubs -> "moved-name") :+ "y")*)
        f      <- fake(withDefault)
        wiring <- TempWiring(f.routes)
        api    <- FakeCcasServer.api(f.routes)
        _      <- ZIO.attemptBlocking(Files.createDirectories(wiring.clubsFile.getParent))
        _      <- ZIO.attemptBlocking(Files.writeString(wiring.clubsFile, "moved-name\n"))
        _      <- CriteriaCommand.set(wiring, api, None, command, interactive = true)
        posts  <- f.posts.get
        err    <- errors
        cached <- ZIO.attemptBlocking(Files.exists(wiring.clubsFile))
      } yield assertTrue(
        err.contains(
          "note: 'moved-name' now belongs to club #2 (now team-beta) on Chess.com, not to #6 (now old-holder); " +
            "using the club that holds it\n"
        ),
        !cached,
        posts.map(_.criteria.excludeClubs) == List(List(beta.clubId))
      )
    },
    test("a lookup the server fails is named and asked again, and the answers so far are kept") {
      for {
        _ <- TestConsole.feedLines((answersAskedAgain(ExcludeClubs, "flaky", "team-beta", MaxClubs -> "25") :+ "y")*)
        f <- fake(withDefault)
        _ <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("  couldn't look flaky up: "),
        posts.map(p => (p.criteria.excludeClubs, p.criteria.maxClubs)) == List((List(beta.clubId), Some(25)))
      )
    },
    test("digits whose lookup failed are not taken for a mistyped id") {
      for {
        _   <- TestConsole.feedLines((answersAskedAgain(ExcludeClubs, "9999", "team-beta") :+ "y")*)
        f   <- fake(withDefault)
        _   <- set(f, Nil, None, interactive = true)
        out <- output
      } yield assertTrue(out.contains("  couldn't look 9999 up: "), !out.contains("type #9999"))
    },
    test("digits are a slug, and an id takes its #") {
      for {
        _     <- TestConsole.feedLines((answersAskedAgain(ExcludeClubs, "2048", "#2048") :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("  Club not found: 2048 (for club id 2048, type #2048)\nexcludeClubs ["),
        posts.map(_.criteria.excludeClubs) == List(List(ClubId(2048)))
      )
    },
    test("a name's note is shown only once every name on the line is found") {
      for {
        _     <- TestConsole.feedLines((answersAskedAgain(ExcludeClubs, "moved-name, nobody", "moved-name") :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
        err   <- errors
      } yield assertTrue(
        out.contains("  Club not found: nobody\nexcludeClubs ["),
        "note: 'moved-name'".r.findAllIn(err).size == 1,
        posts.map(_.criteria.excludeClubs) == List(List(beta.clubId))
      )
    },
    test("an excluded club the server cannot find is named and asked again") {
      for {
        _     <- TestConsole.feedLines((answersAskedAgain(ExcludeClubs, "nobody", "team-beta") :+ "y")*)
        f     <- fake(withDefault)
        _     <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        out.contains("  Club not found: nobody\nexcludeClubs ["),
        posts.map(_.criteria.excludeClubs) == List(List(beta.clubId))
      )
    },
    test("set with no changes sends nothing") {
      for {
        _     <- TestConsole.feedLines(answers()*)
        f     <- fake(withDefault)
        code  <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(code == 0, posts.isEmpty, out.contains("No changes; nothing saved."), !out.contains("Save?"))
    },
    test("a declined set sends nothing") {
      for {
        _     <- TestConsole.feedLines((answers(MaxClubs -> "25") :+ "n")*)
        f     <- fake(withDefault)
        code  <- set(f, Nil, None, interactive = true)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(code == 0, posts.isEmpty, out.contains("Not saved."))
    },
    test("set says a save reaches later runs, scheduled ones included") {
      for {
        _   <- TestConsole.feedLines((answers(MaxClubs -> "25") :+ "n")*)
        f   <- fake(withDefault)
        _   <- set(f, Nil, None, interactive = true)
        out <- output
      } yield assertTrue(
        out.contains(
          "Saving makes a new version: every later run with team-alpha's criteria \"default\" uses it, " +
            "scheduled runs included.\nSave? [Y/n]"
        )
      )
    },
    test("set prints the saved recap, as the server stored it") {
      for {
        _   <- TestConsole.feedLines((answers(MaxClubs -> "25") :+ "y")*)
        f   <- fake(withDefault)
        _   <- set(f, Nil, None, interactive = true)
        out <- output
      } yield assertTrue(
        out.contains("Saved.\nteam-alpha's criteria \"default\", version 2:"),
        out.contains("  maxClubs              25")
      )
    },
    test("a lookback past the cap is named as capped before anything is saved") {
      for {
        _   <- TestConsole.feedLines((answers(DaysSinceLastInvited -> "1000") :+ "n")*)
        f   <- fake(withDefault)
        _   <- set(f, Nil, None, interactive = true)
        out <- output
      } yield assertTrue(
        out.contains("Capped to the 180-day lookback: daysSinceLastInvited: 1000 → 180"),
        out.contains("  daysSinceLastInvited: 90 → 180")
      )
    },
    test("a save that another changed in between says the recap is a later version") {
      for {
        _   <- TestConsole.feedLines((answers(MaxClubs -> "25") :+ "y")*)
        f   <- fakeWith(withDefault, interloper = true)
        _   <- set(f, Nil, None, interactive = true)
        out <- output
      } yield assertTrue(
        out.contains(
          "Saved, and changed again since; the current version:\nteam-alpha's criteria \"default\", version 3:"
        )
      )
    },
    test("criteria from a file the server would refuse are refused before the operator is asked to save") {
      val refused = stored.copy(dailyMinElo = Some(Elo(1800.toShort)), dailyMaxElo = Some(Elo(1500.toShort)))
      for {
        _     <- TestConsole.feedLines("y")
        f     <- fake(withDefault)
        exit  <- withCriteriaFile(refused.toJson)(file => set(f, Nil, Some(file), interactive = true).exit)
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        failure(exit).exists(_.message == "not saved: dailyElo min (1800) must be <= max (1500)"),
        posts.isEmpty,
        !out.contains("Save?")
      )
    },
    test("input that ends at a yes/no setting is named there, nothing more is asked, and nothing is saved") {
      for {
        _     <- TestConsole.feedLines(answers().take(NationalityExclude)*)
        f     <- fake(withDefault)
        exit  <- set(f, Nil, None, interactive = true).exit
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        failure(exit).exists(_.message == "input ended before every setting was answered"),
        out.endsWith("nationalityExclude [no] (y/n, Enter=keep): "),
        posts.isEmpty
      )
    },
    test("a criteria name longer than the server keeps is refused before anything is asked") {
      for {
        f        <- fake(withDefault)
        exit     <- set(f, List("x" * 65), None, interactive = true).exit
        requests <- f.requests.get
      } yield assertTrue(
        failure(exit).exists(e => e.exitCode == 2 && e.message == "criteria name must be <= 64 chars (got 65)"),
        requests == 0
      )
    },
    test("set with no terminal and no --file is a usage error that sends nothing") {
      for {
        f        <- fake(withDefault)
        exit     <- set(f, Nil, None, interactive = false).exit
        requests <- f.requests.get
      } yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists {
          case e: CliError => e.exitCode == 2 && e.message.contains("--file")
          case _           => false
        },
        requests == 0
      )
    },
    test("a second criteria name is a usage error naming the options to move before it") {
      for {
        flagAfter <- CriteriaCommand.criteriaName(List("default", "--club", "team-alpha")).exit
        twoNames  <- CriteriaCommand.criteriaName(List("default", "weekend", "--json")).exit
        namesOnly <- CriteriaCommand.criteriaName(List("default", "weekend")).exit
      } yield {
        def message(exit: zio.Exit[CliError, String]) =
          exit.causeOption.flatMap(_.failureOption).filter(_.exitCode == 2).map(_.message)
        assertTrue(
          message(flagAfter).contains(
            "expected at most one criteria name, got 3 (default, --club, team-alpha); " +
              "options must come before the name: move '--club team-alpha' before 'default'"
          ),
          message(twoNames).exists(_.endsWith("move '--json' before 'default'")),
          message(namesOnly).contains("expected at most one criteria name, got 2 (default, weekend)")
        )
      }
    }
  )
}
