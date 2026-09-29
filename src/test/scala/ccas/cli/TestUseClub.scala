package ccas.cli

import java.nio.file.{Files, Path}

import zio.{ExitCode, Task, UIO, ZIO}
import zio.http.{handler, Method, Response, Routes, Status}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.cli.config.{CliConfig, ConfigWriter}
import ccas.server.routes.ManagedClubRoutes.ManagedClubResponse

/** Tests [[UseClub]]: argument validation, show and clear, and a whole *set* — driven through a temp config and cache
  * and a [[FakeCcasServer]] answering the two questions a set asks, so each case pins what the user sees and what is
  * stored. The pure helpers behind the name-matching fallback and the offline hint are pinned directly.
  */
object TestUseClub extends ZIOSpecDefault {

  // Validation cases return before any write or request, so they can go through `run` and its real wiring.
  private val Server = "http://127.0.0.1:8080"

  private def managed(clubId: Long, slug: String): ManagedClubResponse =
    ManagedClubResponse(clubId = clubId, slug = slug, name = s"Club $clubId", markedAt = "2026-09-21T00:00:00Z")

  private def tempDir: UIO[Path] =
    ZIO.attemptBlocking {
      val dir = Files.createTempDirectory("ccas-use-club")
      dir.toFile.deleteOnExit()
      dir
    }.orDie

  private def wiringIn(dir: Path, server: Routes[Any, Response]): UseClub.Wiring =
    UseClub.Wiring(
      configFile = dir.resolve("config.conf"),
      clubsFile = dir.resolve("clubs.txt"),
      clientLayer = FakeCcasServer.layer(server)
    )

  /** A server that answers `resolution` when asked about `slug` — or lacks the route, as one older than it does — and
    * lists `managedOption` as the managed clubs, failing that request when there is none.
    */
  private def server(
    slug: String,
    resolutionOption: Option[ClubResolution],
    managedOption: Option[List[ManagedClubResponse]]
  ): Routes[Any, Response] = {
    val managedClubs = Method.GET / "api" / "managed-clubs" ->
      handler(managedOption.fold(Response.status(Status.InternalServerError))(FakeCcasServer.json(_)))
    val asked = ClubQuery.BySlug(ClubSlug(slug))
    resolutionOption.fold(Routes(managedClubs))(resolution =>
      Routes(FakeCcasServer.resolving(asked, resolution, "Some Club"), managedClubs)
    )
  }

  /** What a set left behind: its exit code, what it printed, the pointer it stored and the club list it cached. */
  private final case class SetResult(
    code: ExitCode,
    out: Vector[String],
    err: Vector[String],
    storedOption: Option[String],
    cachedOption: Option[List[String]]
  )

  private def setAgainst(server: Routes[Any, Response], slug: String): Task[SetResult] =
    tempDir.flatMap(setIn(_, server, slug))

  private def setIn(dir: Path, server: Routes[Any, Response], slug: String): Task[SetResult] = {
    val wiring = wiringIn(dir, server)
    for {
      code <- UseClub.runWith(wiring)(
        slugs = List(slug),
        clear = false,
        currentClubOption = None,
        server = FakeCcasServer.Url
      )
      out    <- TestConsole.output
      err    <- TestConsole.outputErr
      config <- CliConfig.load(wiring.configFile).mapError(new RuntimeException(_))
      cached <- CompletionCache.readClubsIn(wiring.clubsFile)
    } yield SetResult(code = code, out = out, err = err, storedOption = config.currentClubOption, cachedOption = cached)
  }

  private val beta  = NamedClub(ClubId(7), ClubSlug("team-beta"))
  private val alpha = ClubSlug("team-alpha")

  override def spec: Spec[Any, Any] = suite("TestUseClub")(
    test("blank slug is rejected with exit 2 (no write, no probe)") {
      UseClub.run(slugs = List("   "), clear = false, currentClubOption = None, server = Server)
        .map(code => assertTrue(code == ExitCode(2)))
    },
    test("a slug together with --clear is rejected with exit 2 (conflicting intent)") {
      UseClub.run(slugs = List("team-alpha"), clear = true, currentClubOption = Some("team-beta"), server = Server)
        .map(code => assertTrue(code == ExitCode(2)))
    },
    test("no slug prints the current club") {
      for {
        code <- UseClub.run(slugs = Nil, clear = false, currentClubOption = Some("team-alpha"), server = Server)
        out  <- TestConsole.output
      } yield assertTrue(code == ExitCode.success, out == Vector("team-alpha\n"))
    },
    test("no slug with no current club exits 2 and says how to set one") {
      for {
        code <- UseClub.run(slugs = Nil, clear = false, currentClubOption = None, server = Server)
        err  <- TestConsole.outputErr
      } yield assertTrue(code == ExitCode(2), err.exists(_.contains("ccas use-club <slug>")))
    },
    test("--clear with nothing set succeeds without writing") {
      for {
        code <- UseClub.run(slugs = Nil, clear = true, currentClubOption = None, server = Server)
        out  <- TestConsole.output
      } yield assertTrue(code == ExitCode.success, out == Vector("no current club set\n"))
    },
    test("--clear drops the pointer and names the club it pointed at") {
      for {
        dir <- tempDir
        wiring = wiringIn(dir, Routes.empty)
        _ <- ConfigWriter.setCurrentClub(wiring.configFile, Some(ClubId(7)), "team-beta")
        code <- UseClub.runWith(wiring)(
          slugs = Nil,
          clear = true,
          currentClubOption = Some("7:team-beta"),
          server = FakeCcasServer.Url
        )
        out    <- TestConsole.output
        config <- CliConfig.load(wiring.configFile).mapError(new RuntimeException(_))
      } yield assertTrue(
        code == ExitCode.success,
        out == Vector("current club cleared (was team-beta)\n"),
        config.currentClubOption.isEmpty
      )
    },
    // Arity rejection — without it these silently set the first slug and discard the rest.
    test("two slugs are rejected with exit 2, naming both") {
      for {
        code <- UseClub.run(slugs = List("team-a", "team-b"), clear = false, currentClubOption = None, server = Server)
        err  <- TestConsole.outputErr
      } yield assertTrue(code == ExitCode(2), err.exists(e => e.contains("team-a") && e.contains("team-b")))
    },
    // The case that used to silently SET the club the user asked to clear: zio-cli swallows an option written after a
    // positional, so `--clear` arrives as a second slug. The message must point at the working ordering.
    test("a flag swallowed as a second positional is rejected with the correct ordering hinted") {
      for {
        code <- UseClub.run(
          slugs = List("team-alpha", "--clear"),
          clear = false,
          currentClubOption = None,
          server = Server
        )
        err  <- TestConsole.outputErr
      } yield assertTrue(
        code == ExitCode(2),
        err.exists(_.contains("ccas use-club --clear team-alpha"))
      )
    },
    // A set stores the club the server says the name reaches, by id under its current name, and matches managed status
    // by id — so a former name neither leaves the pointer bare nor reads as unmanaged (#271).
    suite("set")(
      test("a former name of a managed club is stored as that club, with a note, and caches the managed list") {
        val renamed = ClubResolution.Renamed(beta, alpha)
        setAgainst(server("team-alpha", Some(renamed), Some(List(managed(7, "team-beta")))), "team-alpha").map(set =>
          assertTrue(
            set.code == ExitCode.success,
            set.storedOption.contains("7:team-beta"),
            set.err == Vector("note: 'team-alpha' is a former name of 'team-beta'; using 'team-beta'\n"),
            set.out == Vector("current club set to team-beta\n"),
            set.cachedOption.contains(List("team-beta"))
          )
        )
      },
      test("a club you don't manage is stored by id, with a warning saying how to manage it") {
        val gamma = ClubResolution.Known(NamedClub(ClubId(9), ClubSlug("team-gamma")))
        setAgainst(server("team-gamma", Some(gamma), Some(List(managed(7, "team-beta")))), "team-gamma").map(set =>
          assertTrue(
            set.storedOption.contains("9:team-gamma"),
            set.err == Vector(
              "warning: 'team-gamma' is not one of your managed clubs — commands without --club still target it; " +
                "manage it with 'ccas club add team-gamma'\n"
            )
          )
        )
      },
      // A managed club can still show a name it has lost, since `club.slug` is not unique (#254).
      test("a name that moved to another club is not managed because a managed club still shows it") {
        val moved = ClubResolution.Moved(
          club = NamedClub(ClubId(8), alpha),
          requested = alpha,
          previous = List(ClubRef.of(beta))
        )
        setAgainst(server("team-alpha", Some(moved), Some(List(managed(7, "team-alpha")))), "team-alpha").map(set =>
          assertTrue(
            set.storedOption.contains("8:team-alpha"),
            set.err.size == 2,
            set.err.head.contains("now belongs to club #8"),
            set.err.last.contains("'team-alpha' is not one of your managed clubs")
          )
        )
      },
      // The case that used to read "the server did not answer": the club is confirmed, so an unanswered managed list is
      // no reason to warn.
      test("a club the server names raises no warning when the managed list goes unanswered") {
        val known = ClubResolution.Known(NamedClub(ClubId(7), alpha))
        setAgainst(server("team-alpha", Some(known), None), "team-alpha").map(set =>
          assertTrue(set.storedOption.contains("7:team-alpha"), set.err.isEmpty)
        )
      },
      test("a name the server doesn't know is set anyway, with a warning to check the spelling") {
        val notLocal = ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("team-alpah")))
        setAgainst(server("team-alpah", Some(notLocal), Some(Nil)), "team-alpah").map(set =>
          assertTrue(
            set.code == ExitCode.success,
            set.storedOption.contains("team-alpah"),
            set.err == Vector(
              "warning: the server knows no club named 'team-alpah' — check the spelling; commands without --club " +
                "will fail until it does\n"
            ),
            set.out == Vector("current club set to team-alpah\n")
          )
        )
      },
      test("a former name of a club that has since lost its name says commands can't reach it") {
        val problematic = ClubResolution.Problematic(ClubQuery.BySlug(alpha))
        setAgainst(server("team-alpha", Some(problematic), Some(Nil)), "team-alpha").map(set =>
          assertTrue(
            set.storedOption.contains("team-alpha"),
            set.err.size == 1,
            set.err.head.contains("the club once named 'team-alpha' has lost its name to another club")
          )
        )
      },
      // The server's own reason for an ambiguous name suggests `--club-id`, which `use-club` would reject.
      test("an ambiguous name lists its holders by current name, without pointing at a flag use-club lacks") {
        val gamma     = ClubRef(ClubId(8), Some(ClubSlug("team-gamma")))
        val ambiguous = ClubResolution.Ambiguous(alpha, List(ClubRef.of(beta), gamma))
        setAgainst(server("team-alpha", Some(ambiguous), Some(Nil)), "team-alpha").map(set =>
          assertTrue(
            set.storedOption.contains("team-alpha"),
            set.err.size == 1,
            set.err.head.contains("#7 (now team-beta), #8 (now team-gamma)"),
            !set.err.head.contains("--club-id")
          )
        )
      },
      test("a server without the resolve route falls back to the managed clubs' names") {
        setAgainst(server("team-alpha", None, Some(List(managed(7, "team-alpha")))), "team-alpha").map(set =>
          assertTrue(set.storedOption.contains("7:team-alpha"), set.err.isEmpty)
        )
      },
      // By name alone a former name of a managed club looks unmanaged too, so the warning claims no more than that.
      test("the fallback says only that no managed club has the name now") {
        setAgainst(server("team-alpha", None, Some(List(managed(7, "team-beta")))), "team-alpha").map(set =>
          assertTrue(
            set.storedOption.contains("team-alpha"),
            set.err == Vector(
              "warning: no club you manage is named 'team-alpha' now — commands without --club still target it\n"
            )
          )
        )
      },
      test("with no answer at all, the cached club list is the only check") {
        for {
          dir <- tempDir
          _   <- CompletionCache.writeClubsIn(dir.resolve("clubs.txt"), List("team-beta"))
          set <- setIn(dir, server("team-alpha", None, None), "team-alpha")
        } yield assertTrue(
          set.code == ExitCode.success,
          set.storedOption.contains("team-alpha"),
          set.err == Vector(
            "warning: the server didn't answer to check 'team-alpha', and it isn't in the cached club list — " +
              "check the spelling\n"
          )
        )
      }
    ),
    // matchedId is the fallback's id. `club.slug` carries no unique index since #254, so the managed set can show one
    // name twice — and then no id is safe to store.
    test("matchedId: the one managed club answering to the slug, case-insensitively") {
      assertTrue(UseClub.matchedId("Team-Alpha", Some(List(managed(7, "team-alpha"), managed(8, "team-beta"))))
        .contains(ClubId(7)))
    },
    test("matchedId: two managed clubs showing one slug upgrade nothing") {
      val ambiguous = Some(List(managed(7, "team-alpha"), managed(8, "team-alpha")))
      assertTrue(UseClub.matchedId("team-alpha", ambiguous).isEmpty)
    },
    test("matchedId: an unmatched slug, and an unreachable server, upgrade nothing") {
      assertTrue(
        UseClub.matchedId("team-gamma", Some(List(managed(7, "team-alpha")))).isEmpty,
        UseClub.matchedId("team-alpha", None).isEmpty
      )
    },
    // offlineHint's `None` branch is the only consumer of the Option `readClubsIn` returns — without it, collapsing that
    // back to a bare list would compile and break nothing.
    suite("offlineHint")(
      test("an unreadable cache is reported, not treated as empty") {
        for {
          _   <- UseClub.offlineHint("team-alpha", None)
          err <- TestConsole.outputErr
        } yield assertTrue(err.exists(_.contains("the club cache couldn't be read")))
      },
      test("a read cache that lacks the slug says to check the spelling") {
        for {
          _   <- UseClub.offlineHint("team-gamma", Some(List("team-alpha")))
          err <- TestConsole.outputErr
        } yield assertTrue(err.exists(_.contains("check the spelling")))
      },
      test("a read cache that has the slug says nothing") {
        for {
          _   <- UseClub.offlineHint("team-alpha", Some(List("team-alpha")))
          err <- TestConsole.outputErr
        } yield assertTrue(err.isEmpty)
      },
      test("an empty cache says nothing — absence proves nothing") {
        for {
          _   <- UseClub.offlineHint("team-alpha", Some(Nil))
          err <- TestConsole.outputErr
        } yield assertTrue(err.isEmpty)
      }
    ),
    // isUnknown is the predicate behind offlineHint's Some branch.
    test("isUnknown: an empty cache never warns") {
      assertTrue(!UseClub.isUnknown("team-alpha", Nil))
    },
    test("isUnknown: an exact hit does not warn") {
      assertTrue(!UseClub.isUnknown("team-alpha", List("team-beta", "team-alpha")))
    },
    test("isUnknown: a case-differing hit does not warn (ClubSlug lowercases anyway)") {
      assertTrue(!UseClub.isUnknown("Team-Alpha", List("team-alpha")))
    },
    test("isUnknown: a genuine miss warns") {
      assertTrue(UseClub.isUnknown("team-gamma", List("team-alpha", "team-beta")))
    }
  )
}
