package ccas.cli

import zio.{ExitCode, Ref, Task, UIO, ZIO}
import zio.http.{handler, string, Method, Request, Response, Route, Routes, Status}
import zio.json.DecoderOps
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubResolution, NamedClub}
import ccas.analysis.apps.recruitment.CriteriaSpec
import ccas.analysis.tables.RecruitmentCriteria
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.{ClubRequest, ClubResult}
import ccas.server.routes.JobRoutes.{ClubJobResult, RecruitmentRequest}
import ccas.server.routes.RecruitmentCriteriaRoutes.CriteriaView

/** An interactive `ccas recruit` shows the criteria it would run under and asks before submitting (#485): through
  * [[Dispatcher.confirmCriteria]] with a submit that records what it was asked to run, and through
  * [[Dispatcher.dispatch]] for a run with no terminal, against a [[FakeCcasServer]] that knows one alias of one club.
  */
object TestRecruitConfirm extends ZIOSpecDefault {

  private val alpha       = NamedClub(ClubId(1), ClubSlug("team-alpha"))
  private val byAlphaName = ClubQuery.BySlug(alpha.slug)
  private val named       = ClubTarget(byAlphaName, "team-alpha")
  private val recap = List("Criteria for alias default, set 2026-03-18:", "  maxClubs  40  in at most this many clubs")

  // Answers the criteria of alias `default` for the club named `team-alpha` as `resolution` resolves it, and 400 for
  // anything else, so a case pins the alias and the club the command asked about. Counts the requests it answers.
  private def criteriaRoute(resolution: ClubResolution, asked: Ref[Int]): Route[Any, Nothing] =
    Method.GET / "api" / "recruitment-criteria" / string("alias") -> handler { (alias: String, req: Request) =>
      val view = CriteriaView(CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily), recap)
      asked.update(_ + 1) *> ClubRequest.query(req).option.map { queryOption =>
        if (alias == "default" && queryOption.contains(byAlphaName)) {
          FakeCcasServer.json(ClubResult(byAlphaName.describe, resolution, resolution.runnable.toOption.map(_ => view)))
        } else { Response.status(Status.BadRequest) }
      }
    }

  /** Runs the confirmation with a submit that records the club it was asked to run against, and answers 7. */
  private def confirm(resolution: ClubResolution): Task[(Int, List[ClubTarget])] =
    for {
      asked     <- Ref.make(0)
      submitted <- Ref.make(List.empty[ClubTarget])
      routes = Routes(criteriaRoute(resolution, asked))
      wiring <- TempWiring(routes)
      api    <- FakeCcasServer.api(routes)
      code <- Dispatcher.confirmCriteria(wiring, api, None, named, "default") { target =>
        submitted.update(_ :+ target).as(7)
      }
      targets <- submitted.get
    } yield (code, targets)

  // The recruit submit route, recording each request's club and answering that no job started, so nothing is followed.
  private def recruitSubmit(sent: Ref[List[ClubQuery]]): Route[Any, Nothing] =
    Method.POST / "api" / "jobs" / "recruitment" -> handler { (req: Request) =>
      for {
        body <- req.body.asString.orElseSucceed("")
        _    <- ZIO.foreachDiscard(body.fromJson[RecruitmentRequest].toOption)(r => sent.update(_ :+ r.club))
      } yield FakeCcasServer.json(
        ClubJobResult(byAlphaName.describe, None, Some("refused by the test"), ClubResolution.Known(alpha))
      )
    }

  private def recruit: CliCommand.Recruit =
    CliCommand.Recruit(
      server = FakeCcasServer.Url,
      club = Some("team-alpha"),
      clubIdOption = None,
      alias = None,
      target = None,
      cumulative = false,
      sourceClubs = Nil,
      timeLimitMinutes = None,
      explore = None,
      stdout = false,
      report = false,
      runId = None,
      noProgress = true
    )

  private def output: UIO[String] = TestConsole.output.map(_.mkString)

  override def spec: Spec[Any, Throwable] = suite("TestRecruitConfirm")(
    test("a declined confirmation starts no run and names the command that edits the criteria") {
      for {
        _                   <- TestConsole.feedLines("n")
        (code, submittedTo) <- confirm(ClubResolution.Known(alpha))
        out                 <- output
      } yield assertTrue(
        code == 0,
        submittedTo.isEmpty,
        out.contains(recap.mkString("\n")),
        out.contains("Run with these criteria? [Y/n]"),
        out.contains("Not started. Edit the criteria with: "),
        out.contains("RecruitmentCriteriaApp set team-alpha default")
      )
    },
    test("a confirmed run is submitted, by id, to the club settled while confirming") {
      val renamed = ClubResolution.Renamed(NamedClub(ClubId(1), ClubSlug("team-alpha-now")), alpha.slug)
      for {
        _                   <- TestConsole.feedLines("")
        (code, submittedTo) <- confirm(renamed)
        out                 <- output
      } yield assertTrue(
        code == 7,
        submittedTo.map(_.query) == List(ClubQuery.ById(ClubId(1))),
        !out.contains("Not started.")
      )
    },
    test("criteria the server has no club for stop the run before it asks") {
      for {
        exit <- confirm(ClubResolution.NotLocal(byAlphaName)).exit
        out  <- output
      } yield assertTrue(exit.isFailure, !out.contains("Run with these criteria?"))
    },
    test("a recruit with no terminal submits without reading or asking about the criteria") {
      for {
        asked <- Ref.make(0)
        sent  <- Ref.make(List.empty[ClubQuery])
        routes = Routes(criteriaRoute(ClubResolution.Known(alpha), asked), recruitSubmit(sent))
        wiring    <- TempWiring(routes)
        code      <- Dispatcher.dispatch(wiring)(recruit, None)
        askedFor  <- asked.get
        submitted <- sent.get
        out       <- output
      } yield assertTrue(
        code == ExitCode(1),
        askedFor == 0,
        submitted == List(byAlphaName),
        !out.contains("Run with these criteria?")
      )
    }
  )
}
