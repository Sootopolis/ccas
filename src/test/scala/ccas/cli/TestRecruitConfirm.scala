package ccas.cli

import zio.{durationInt, ExitCode, Ref, Task, UIO, ZIO}
import zio.http.{handler, string, Method, Request, Response, Route, Routes, Status}
import zio.json.DecoderOps
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubResolution, NamedClub}
import ccas.analysis.apps.recruitment.CriteriaSpec
import ccas.analysis.tables.RecruitmentCriteria
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.{ClubRequest, ClubResult}
import ccas.server.routes.JobRoutes.{ClubJobResult, InvitedUsernames, RecruitmentRequest}
import ccas.server.routes.RecruitmentCriteriaRoutes.CriteriaView

/** An interactive `ccas recruit` shows the criteria it would run under and asks before submitting, then asks before
  * marking invites (#485), calling the operator back to that second question (#533): through
  * [[Dispatcher.confirmCriteria]] with a submit that records what it was asked to run, through [[Dispatcher.scout]]
  * for what a confirmed run sends, through [[Dispatcher.dispatch]] for a run with no terminal, and through
  * [[Dispatcher.confirmFlow]] for the invite prompt, against a [[FakeCcasServer]] that knows one set of criteria of
  * one club.
  */
object TestRecruitConfirm extends ZIOSpecDefault {

  private val alpha       = NamedClub(ClubId(1), ClubSlug("team-alpha"))
  private val byAlphaName = ClubQuery.BySlug(alpha.slug)
  private val named       = ClubTarget(byAlphaName, "team-alpha")
  private val shownId     = 7L
  private val recap =
    List("team-alpha's criteria \"default\", version of 2026-03-18:", "  maxClubs  40  in at most this many clubs")

  // Answers the criteria named `default` for the club named `team-alpha` as `resolution` resolves it, and 400 for
  // anything else, so a case pins the name and the club the command asked about. Counts the requests it answers.
  private def criteriaRoute(resolution: ClubResolution, asked: Ref[Int]): Route[Any, Nothing] =
    Method.GET / "api" / "recruitment-criteria" / string("name") -> handler { (name: String, req: Request) =>
      val view = CriteriaView(Some(shownId), CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily), recap)
      asked.update(_ + 1) *> ClubRequest.query(req).option.map { queryOption =>
        if (name == "default" && queryOption.contains(byAlphaName)) {
          FakeCcasServer.json(ClubResult(byAlphaName.describe, resolution, resolution.runnable.toOption.map(_ => view)))
        } else { Response.status(Status.BadRequest) }
      }
    }

  /** Runs the confirmation with a submit that records the club and criteria it was asked to run, and answers 7. */
  private def confirm(
    resolution: ClubResolution,
    nonDefaultServerOption: Option[String]
  ): Task[(Int, List[(ClubTarget, Option[Long])])] =
    for {
      asked     <- Ref.make(0)
      submitted <- Ref.make(List.empty[(ClubTarget, Option[Long])])
      routes = Routes(criteriaRoute(resolution, asked))
      wiring <- TempWiring(routes)
      api    <- FakeCcasServer.api(routes)
      code <- Dispatcher.confirmCriteria(wiring, api, None, nonDefaultServerOption, named, "default") {
        (target, criteriaId) =>
          submitted.update(_ :+ (target -> criteriaId)).as(7)
      }
      targets <- submitted.get
    } yield (code, targets)

  // The recruit submit route, recording each request and answering that no job started, so nothing is followed.
  private def recruitSubmit(sent: Ref[List[RecruitmentRequest]]): Route[Any, Nothing] =
    Method.POST / "api" / "jobs" / "recruitment" -> handler { (req: Request) =>
      for {
        body <- req.body.asString.orElseSucceed("")
        _    <- ZIO.foreachDiscard(body.fromJson[RecruitmentRequest].toOption)(r => sent.update(_ :+ r))
      } yield FakeCcasServer.json(
        ClubJobResult(byAlphaName.describe, None, Some("refused by the test"), ClubResolution.Known(alpha))
      )
    }

  private def recruit: CliCommand.Recruit =
    CliCommand.Recruit(
      server = FakeCcasServer.Url,
      club = Some("team-alpha"),
      clubIdOption = None,
      criteriaNameOption = None,
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
        (code, submittedTo) <- confirm(ClubResolution.Known(alpha), None)
        out                 <- output
      } yield assertTrue(
        code == 0,
        submittedTo.isEmpty,
        out.contains(recap.mkString("\n")),
        out.contains("Run with these criteria? [Y/n]"),
        out.contains("Not started. Edit the criteria with: "),
        out.contains("ccas recruitment-criteria set --club team-alpha default")
      )
    },
    test("a declined confirmation against a server other than the default names it in the command") {
      for {
        _   <- TestConsole.feedLines("n")
        _   <- confirm(ClubResolution.Known(alpha), Some("http://localhost:8081"))
        out <- output
      } yield assertTrue(
        out.contains("ccas recruitment-criteria set --server http://localhost:8081 --club team-alpha default")
      )
    },
    test("a confirmed run is submitted, by id, to the club settled while confirming") {
      val renamed = ClubResolution.Renamed(NamedClub(ClubId(1), ClubSlug("team-alpha-now")), alpha.slug)
      for {
        _                   <- TestConsole.feedLines("")
        (code, submittedTo) <- confirm(renamed, None)
        out                 <- output
      } yield assertTrue(
        code == 7,
        submittedTo.map(_._1.query) == List(ClubQuery.ById(ClubId(1))),
        !out.contains("Not started.")
      )
    },
    test("a confirmed run sends the version of the criteria it showed") {
      for {
        _     <- TestConsole.feedLines("y")
        asked <- Ref.make(0)
        sent  <- Ref.make(List.empty[RecruitmentRequest])
        routes = Routes(criteriaRoute(ClubResolution.Known(alpha), asked), recruitSubmit(sent))
        wiring <- TempWiring(routes)
        api    <- FakeCcasServer.api(routes)
        follower = JobFollower(api, 1.minute, 1.milli, 3, showProgress = false, wiring.recentJobsFile)
        _         <- Dispatcher.scout(wiring, api, follower, recruit, None, None, interactiveConfirm = true)
        submitted <- sent.get
      } yield assertTrue(submitted.map(_.confirmedCriteriaId) == List(Some(shownId)))
    },
    test("criteria the server has no club for stop the run before it asks") {
      for {
        exit <- confirm(ClubResolution.NotLocal(byAlphaName), None).exit
        out  <- output
      } yield assertTrue(exit.isFailure, !out.contains("Run with these criteria?"))
    },
    test("a recruit with no terminal submits without reading, asking about or vouching for the criteria") {
      for {
        asked <- Ref.make(0)
        sent  <- Ref.make(List.empty[RecruitmentRequest])
        routes = Routes(criteriaRoute(ClubResolution.Known(alpha), asked), recruitSubmit(sent))
        wiring    <- TempWiring(routes)
        code      <- Dispatcher.dispatch(wiring)(recruit, None, FakeCcasServer.Url)
        askedFor  <- asked.get
        submitted <- sent.get
        out       <- output
      } yield assertTrue(
        code == ExitCode(1),
        askedFor == 0,
        submitted.map(r => (r.club, r.confirmedCriteriaId)) == List((byAlphaName, None)),
        !out.contains("Run with these criteria?")
      )
    },
    test("the invite prompt names the club the candidates are invited to") {
      for {
        prompted <- invitePrompt(List("alice"))
      } yield assertTrue(
        prompted.output.contains("Mark all 1 as invited to team-alpha and copy to clipboard? [Y/n]"),
        prompted.output.contains("Left 1 as deferred")
      )
    },
    test("the invite prompt alerts as it opens, naming how many candidates wait and for which club") {
      for {
        prompted <- invitePrompt(List("alice", "bob"))
      } yield assertTrue(prompted.alerts == List("2 found for team-alpha: confirm the invites"))
    },
    test("a run that found no candidates raises no alert") {
      for {
        prompted <- invitePrompt(Nil)
      } yield assertTrue(prompted.output.contains("No candidates found."), prompted.alerts.isEmpty)
    }
  )

  private case class Prompted(output: String, alerts: List[String])

  /** Runs the invite prompt for a job that found `names`, declining it, and returns what it printed and each alert. */
  private def invitePrompt(names: List[String]): Task[Prompted] = {
    val found = Method.GET / "api" / "jobs" / string("jobId") / "recruitment" / "found" -> handler {
      (_: String, _: Request) => FakeCcasServer.json(InvitedUsernames(names))
    }
    for {
      alerts <- Ref.make(List.empty[String])
      _      <- TestConsole.feedLines("n")
      api    <- FakeCcasServer.api(Routes(found))
      _      <- Dispatcher.confirmFlow(api, "team-alpha", "job-1", text => alerts.update(_ :+ text))
      out    <- output
      sent   <- alerts.get
    } yield Prompted(out, sent)
  }
}
