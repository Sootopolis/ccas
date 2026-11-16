package ccas.cli

import java.nio.file.Files
import java.time.Instant

import zio.{Ref, Task, UIO, ZIO}
import zio.http.{handler, string, Method, Request, Response, Routes, Status}
import zio.json.{DecoderOps, EncoderOps, JsonCodec}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubResolution, NamedClub}
import ccas.analysis.apps.recruitment.CriteriaSpec
import ccas.analysis.tables.RecruitmentCriteria
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.ClubResult
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
    } yield Fake(criteria, posts, requests, interloper)

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

  // `json` as a criteria file for `use`, deleted afterwards.
  private def withCriteriaFile[A](json: String)(use: String => Task[A]): Task[A] =
    ZIO.acquireReleaseWith(ZIO.attemptBlocking(Files.writeString(Files.createTempFile("criteria", ".json"), json)))(
      file => ZIO.attemptBlocking(Files.deleteIfExists(file)).ignore
    )(file => use(file.toString))

  private def failure(exit: zio.Exit[Throwable, Int]): Option[CliError] =
    exit.causeOption.flatMap(_.failureOption).collect { case e: CliError => e }

  private def output: UIO[String] = TestConsole.output.map(_.mkString)

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
    test("criteria the server would refuse are refused before the operator is asked to save") {
      for {
        _     <- TestConsole.feedLines((answers(DailyMinElo -> "1800", DailyMaxElo -> "1500") :+ "y")*)
        f     <- fake(withDefault)
        exit  <- set(f, Nil, None, interactive = true).exit
        posts <- f.posts.get
        out   <- output
      } yield assertTrue(
        failure(exit).exists(_.message == "not saved: dailyElo min (1800) must be <= max (1500)"),
        posts.isEmpty,
        !out.contains("Save?")
      )
    },
    test("input that ends before every setting is answered is named, and nothing is saved") {
      for {
        _     <- TestConsole.feedLines("", "", "")
        f     <- fake(withDefault)
        exit  <- set(f, Nil, None, interactive = true).exit
        posts <- f.posts.get
      } yield assertTrue(
        failure(exit).exists(_.message == "input ended before every setting was answered"),
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
