package ccas.cli

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

import zio.{ExitCode, Task, UIO, ZIO}
import zio.http.{handler, Method, Request, Response, Route, Routes, Status}
import zio.json.{DecoderOps, JsonDecoder}
import zio.test.{assertTrue, Spec, TestAspect, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.cli.config.{CliConfig, ConfigWriter, CurrentClubRef}
import ccas.server.routes.{ClubRequest, ClubResult}
import ccas.server.routes.BlacklistRoutes.BlacklistEntryResponse
import ccas.server.routes.ClubRoutes.{ClubInfo, ClubsResponse}
import ccas.server.routes.JobRoutes.{ClubJobResult, StatsRequest}
import ccas.server.routes.ManagedClubRoutes.{ManagedClubResponse, MarkManagedRequest}

/** Runs whole commands through [[Dispatcher.dispatch]] against a [[FakeCcasServer]] and a [[TempWiring]], and pins what
  * each leaves behind: the `current_club` stored, the club list cached for completion, and the job ids recorded.
  * `blacklist list` stands in for the synchronous commands that name a club and `stats --detach` for a submit, beside
  * `club add` and `remove`, so no case reaches a prompt, a progress bar or the clipboard.
  */
object TestDispatcherEndToEnd extends ZIOSpecDefault {

  private val alphaSlug = ClubSlug("team-alpha")
  private val alpha     = NamedClub(ClubId(1), alphaSlug)
  private val beta      = NamedClub(ClubId(2), ClubSlug("team-beta"))

  private val byAlphaName: ClubQuery = ClubQuery.BySlug(alphaSlug)

  private def managed(club: NamedClub): ManagedClubResponse =
    ManagedClubResponse(
      clubId = ClubId.unwrap(club.clubId),
      slugOption = Some(ClubSlug.unwrap(club.slug)),
      name = s"Club ${ClubId.unwrap(club.clubId)}",
      markedAt = "2026-11-01T00:00:00Z"
    )

  // --- The fake server ---

  // Each club-naming route answers only the query the test expects, and 400 to any other, so a case pins what the
  // command asked as well as what it did with the answer.
  private def onlyFor(asked: ClubQuery, answer: => Response)(queryOption: Option[ClubQuery]): Response =
    if (queryOption.contains(asked)) { answer }
    else { Response.status(Status.BadRequest) }

  private def paramsNaming(req: Request): UIO[Option[ClubQuery]] = ClubRequest.query(req).option

  private def bodyNaming[A: JsonDecoder](req: Request)(club: A => ClubQuery): UIO[Option[ClubQuery]] =
    req.body.asString.map(_.fromJson[A].toOption.map(club)).orElseSucceed(None)

  private def blacklistList(asked: ClubQuery, resolution: ClubResolution): Route[Any, Nothing] =
    Method.GET / "api" / "blacklist" -> handler { (req: Request) =>
      val entries = resolution.runnable.toOption.map(_ => List.empty[BlacklistEntryResponse])
      paramsNaming(req).map(onlyFor(asked, FakeCcasServer.json(ClubResult(asked.describe, resolution, entries))))
    }

  private def clubAdd(asked: ClubQuery, resolution: ClubResolution): Route[Any, Nothing] =
    Method.POST / "api" / "managed-clubs" -> handler { (req: Request) =>
      val added = resolution.runnable.toOption.map(_ => true)
      bodyNaming[MarkManagedRequest](req)(_.club)
        .map(onlyFor(asked, FakeCcasServer.json(ClubResult(asked.describe, resolution, added))))
    }

  private def clubRemove(asked: ClubQuery, resolution: ClubResolution): Route[Any, Nothing] =
    Method.DELETE / "api" / "managed-clubs" -> handler { (req: Request) =>
      val removed = resolution.reachedOption.map(_ => true)
      paramsNaming(req).map(onlyFor(asked, FakeCcasServer.json(ClubResult(asked.describe, resolution, removed))))
    }

  private def statsSubmit(asked: ClubQuery, resolution: ClubResolution, jobId: String): Route[Any, Nothing] =
    Method.POST / "api" / "jobs" / "stats" -> handler { (req: Request) =>
      val result = ClubJobResult(
        club = asked.describe,
        jobIdOption = resolution.runnable.toOption.map(_ => jobId),
        error = None,
        resolution = resolution
      )
      bodyNaming[StatsRequest](req)(_.club).map(onlyFor(asked, FakeCcasServer.json(result)))
    }

  private def managedClubs(clubs: List[NamedClub]): Route[Any, Nothing] =
    Method.GET / "api" / "managed-clubs" -> handler(FakeCcasServer.json(clubs.map(managed)))

  private def allClubs(clubs: List[NamedClub]): Route[Any, Nothing] =
    Method.GET / "api" / "clubs" -> handler {
      FakeCcasServer.json(ClubsResponse(clubs.map(club => ClubInfo(club.slug, "Some Club"))))
    }

  // --- Running a command ---

  /** What a command left behind. `cachedOption` is `None` when there is no clubs cache at all. */
  private final case class Ran(
    code: ExitCode,
    out: Vector[String],
    err: Vector[String],
    pointerOption: Option[String],
    cachedOption: Option[List[String]],
    recentJobs: List[String]
  )

  /** Runs `cmd` with `pointerOption` stored as `current_club`, as `Main` would have loaded it, then reads the files. */
  private def run(wiring: CliWiring, pointerOption: Option[String], cmd: CliCommand.ServerCommand): Task[Ran] =
    for {
      _ <- ZIO.foreachDiscard(pointerOption.map(CurrentClubRef.parse)) { pointer =>
        ConfigWriter.setCurrentClub(wiring.configFile, pointer.clubIdOption, pointer.slug)
      }
      code   <- Dispatcher.dispatch(wiring)(cmd, pointerOption)
      out    <- TestConsole.output
      err    <- TestConsole.outputErr
      config <- CliConfig.load(wiring.configFile).mapError(new RuntimeException(_))
      cached <- linesIfPresent(wiring.clubsFile)
      jobs   <- linesIfPresent(wiring.recentJobsFile)
    } yield Ran(
      code = code,
      out = out,
      err = err,
      pointerOption = config.currentClubOption,
      cachedOption = cached,
      recentJobs = jobs.getOrElse(Nil)
    )

  private def linesIfPresent(file: Path): Task[Option[List[String]]] =
    ZIO.attemptBlocking(Option.when(Files.exists(file))(Files.readAllLines(file).asScala.toList))

  private def listBlacklist(clubOption: Option[String]): CliCommand.BlacklistList =
    CliCommand.BlacklistList(server = FakeCcasServer.Url, club = clubOption, clubIdOption = None)

  private def addClub(slug: String): CliCommand.ClubsAdd =
    CliCommand.ClubsAdd(server = FakeCcasServer.Url, slugs = List(slug), clubIdOption = None)

  private def removeClub(slug: String): CliCommand.ClubsRemove =
    CliCommand.ClubsRemove(server = FakeCcasServer.Url, slugs = List(slug), clubIdOption = None)

  /** Stores `slugs` as a clubs cache written just now, so the TTL alone would keep it. */
  private def freshCache(wiring: CliWiring, slugs: List[String]): UIO[Unit] =
    CompletionCache.writeClubs(wiring.clubsFile, slugs).unit

  override def spec: Spec[Any, Throwable] = suite("TestDispatcherEndToEnd")(
    suite("current_club")(
      test("an id pointer to a renamed club is rewritten with the name the club holds now") {
        val renamed = NamedClub(ClubId(1), ClubSlug("new-name"))
        for {
          wiring <- TempWiring(
            Routes(blacklistList(ClubQuery.ById(ClubId(1)), ClubResolution.Known(renamed)), managedClubs(Nil))
          )
          ran <- run(wiring, Some("1:old-name"), listBlacklist(None))
        } yield assertTrue(ran.code == ExitCode.success, ran.pointerOption.contains("1:new-name"))
      },
      test("a bare pointer gains its club's id from the first command that reaches it") {
        for {
          wiring <- TempWiring(Routes(blacklistList(byAlphaName, ClubResolution.Known(alpha)), managedClubs(Nil)))
          ran    <- run(wiring, Some("team-alpha"), listBlacklist(None))
        } yield assertTrue(ran.code == ExitCode.success, ran.pointerOption.contains("1:team-alpha"))
      },
      // #386: holding the pointer back once did not keep it, so it follows the name and says so.
      test("a bare pointer whose name moved is repointed at the club holding it, and says so") {
        val holder = NamedClub(ClubId(8), alphaSlug)
        val moved  = ClubResolution.Moved(club = holder, requested = alphaSlug, previous = List(ClubRef.of(alpha)))
        for {
          wiring <- TempWiring(Routes(blacklistList(byAlphaName, moved), managedClubs(Nil)))
          ran    <- run(wiring, Some("team-alpha"), listBlacklist(None))
          // The move's own note, then the pointer's, each once; their wording is pinned in TestDispatcherResolution.
          notes = Dispatcher.nameChangeNote(moved).toList ++ Dispatcher.followedNameNote(Some("team-alpha"), moved)
        } yield assertTrue(
          ran.code == ExitCode.success,
          ran.pointerOption.contains("8:team-alpha"),
          notes.size == 2,
          ran.err == notes.map(note => s"$note\n").toVector
        )
      },
      test("a command naming another club with --club leaves current_club alone") {
        val byBetaName = ClubQuery.BySlug(beta.slug)
        for {
          wiring <- TempWiring(Routes(blacklistList(byBetaName, ClubResolution.Known(beta)), managedClubs(Nil)))
          ran    <- run(wiring, Some("team-alpha"), listBlacklist(Some("team-beta")))
        } yield assertTrue(ran.code == ExitCode.success, ran.pointerOption.contains("team-alpha"))
      },
      test("club remove clears a current_club that means the removed club") {
        for {
          wiring <- TempWiring(Routes(clubRemove(byAlphaName, ClubResolution.Known(alpha)), managedClubs(Nil)))
          ran    <- run(wiring, Some("1:team-alpha"), removeClub("team-alpha"))
        } yield assertTrue(
          ran.code == ExitCode.success,
          ran.pointerOption.isEmpty,
          ran.out.exists(_.startsWith("that was your current club; cleared it"))
        )
      },
      test("club remove leaves a current_club that means another club") {
        for {
          wiring <- TempWiring(Routes(clubRemove(byAlphaName, ClubResolution.Known(alpha)), managedClubs(Nil)))
          ran    <- run(wiring, Some("2:team-beta"), removeClub("team-alpha"))
        } yield assertTrue(ran.code == ExitCode.success, ran.pointerOption.contains("2:team-beta"))
      }
    ),
    suite("clubs cache")(
      test("a successful command fills a missing clubs cache with the managed clubs") {
        for {
          wiring <- TempWiring(
            Routes(blacklistList(byAlphaName, ClubResolution.Known(alpha)), managedClubs(List(alpha, beta)))
          )
          ran <- run(wiring, None, listBlacklist(Some("team-alpha")))
        } yield assertTrue(ran.code == ExitCode.success, ran.cachedOption.contains(List("team-alpha", "team-beta")))
      },
      test("with nothing managed, the clubs cache falls back to every club the server knows") {
        for {
          wiring <- TempWiring(
            Routes(
              blacklistList(byAlphaName, ClubResolution.Known(alpha)),
              managedClubs(Nil),
              allClubs(List(alpha, beta))
            )
          )
          ran <- run(wiring, None, listBlacklist(Some("team-alpha")))
        } yield assertTrue(ran.code == ExitCode.success, ran.cachedOption.contains(List("team-alpha", "team-beta")))
      },
      test("an ordinary command leaves a fresh clubs cache alone") {
        for {
          wiring <- TempWiring(
            Routes(blacklistList(byAlphaName, ClubResolution.Known(alpha)), managedClubs(List(beta)))
          )
          _   <- freshCache(wiring, List("team-alpha"))
          ran <- run(wiring, None, listBlacklist(Some("team-alpha")))
        } yield assertTrue(ran.code == ExitCode.success, ran.cachedOption.contains(List("team-alpha")))
      },
      // The command fails on the miss, so its own refresh never runs: the next command's does.
      test("a club the server can't reach drops a fresh clubs cache") {
        for {
          wiring <- TempWiring(
            Routes(blacklistList(byAlphaName, ClubResolution.NotLocal(byAlphaName)), managedClubs(List(beta)))
          )
          _   <- freshCache(wiring, List("team-alpha"))
          ran <- run(wiring, None, listBlacklist(Some("team-alpha")))
        } yield assertTrue(ran.code == ExitCode(1), ran.cachedOption.isEmpty)
      },
      // The command succeeds, so its own refresh finds the cache gone and rewrites it at once.
      test("a former name acted on swaps a fresh clubs cache for the managed clubs") {
        val oldName = ClubSlug("old-name")
        for {
          wiring <- TempWiring(
            Routes(
              blacklistList(ClubQuery.BySlug(oldName), ClubResolution.Renamed(alpha, oldName)),
              managedClubs(List(alpha))
            )
          )
          _   <- freshCache(wiring, List("old-name"))
          ran <- run(wiring, None, listBlacklist(Some("old-name")))
        } yield assertTrue(ran.code == ExitCode.success, ran.cachedOption.contains(List("team-alpha")))
      },
      test("club add rewrites a fresh clubs cache with the managed set") {
        for {
          wiring <- TempWiring(
            Routes(clubAdd(byAlphaName, ClubResolution.Known(alpha)), managedClubs(List(alpha, beta)))
          )
          _   <- freshCache(wiring, List("team-beta"))
          ran <- run(wiring, None, addClub("team-alpha"))
        } yield assertTrue(
          ran.code == ExitCode.success,
          ran.out == Vector("now managing team-alpha\n"),
          ran.cachedOption.contains(List("team-alpha", "team-beta"))
        )
      },
      // #181: an unwritable cache directory would otherwise leave completion dead with nothing said.
      test("club add says when the clubs cache can't be written, and still succeeds") {
        for {
          dir     <- TempWiring.tempDir
          blocker <- TempWiring.blocker(dir)
          wiring = TempWiring
            .in(dir, Routes(clubAdd(byAlphaName, ClubResolution.Known(alpha)), managedClubs(List(alpha))))
            .copy(clubsFile = blocker.resolve("clubs.txt"))
          ran <- run(wiring, None, addClub("team-alpha"))
        } yield assertTrue(
          ran.code == ExitCode.success,
          ran.err.exists(_.startsWith(s"warning: could not update the club cache at ${wiring.clubsFile}"))
        )
      }
    ),
    test("a detached submit records its job id for completion") {
      val stats = CliCommand.Stats(
        server = FakeCcasServer.Url,
        club = Some("team-alpha"),
        clubIdOption = None,
        since = None,
        until = None,
        noProgress = false,
        detach = true
      )
      for {
        wiring <- TempWiring(Routes(statsSubmit(byAlphaName, ClubResolution.Known(alpha), "job-7"), managedClubs(Nil)))
        ran    <- run(wiring, None, stats)
      } yield assertTrue(
        ran.code == ExitCode.success,
        ran.out.exists(_.contains("submitted (detached): job-7")),
        ran.recentJobs == List("job-7")
      )
    }
  ) @@ TestAspect.withLiveClock
}
