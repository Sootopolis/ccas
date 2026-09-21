package ccas.cli

import zio.ExitCode
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.api.misc.subtypes.ClubId
import ccas.server.routes.ManagedClubRoutes.ManagedClubResponse

/** Tests the [[UseClub]] modes and helpers that touch neither the filesystem nor the network: argument validation, the
  * show/clear paths that short-circuit before any write, the classify/matchedId/isUnknown helpers, and the advisories.
  *
  * The `use-club <slug>` *set* path is deliberately not driven here — it resolves through `XdgPaths.configFile` and
  * would clobber the developer's real `~/.config/ccas/config.conf`, and its probe hits a real server. The write
  * mechanics are covered in `TestConfigWriter`; a dummy server is passed because none of these cases reach the probe.
  */
object TestUseClub extends ZIOSpecDefault {

  private val Server = "http://127.0.0.1:8080"

  private def managed(clubId: Long, slug: String): ManagedClubResponse =
    ManagedClubResponse(clubId = clubId, slug = slug, name = s"Club $clubId", markedAt = "2026-09-21T00:00:00Z")

  override def spec: Spec[Any, Any] = suite("TestUseClub")(
    test("blank slug is rejected with exit 2 (no write, no probe)") {
      UseClub.run(List("   "), clear = false, None, Server).map(code => assertTrue(code == ExitCode(2)))
    },
    test("a slug together with --clear is rejected with exit 2 (conflicting intent)") {
      UseClub.run(List("team-alpha"), clear = true, Some("team-beta"), Server)
        .map(code => assertTrue(code == ExitCode(2)))
    },
    test("no slug prints the current club") {
      for {
        code <- UseClub.run(Nil, clear = false, Some("team-alpha"), Server)
        out  <- TestConsole.output
      } yield assertTrue(code == ExitCode.success, out == Vector("team-alpha\n"))
    },
    test("no slug with no current club exits 2 and says how to set one") {
      for {
        code <- UseClub.run(Nil, clear = false, None, Server)
        err  <- TestConsole.outputErr
      } yield assertTrue(code == ExitCode(2), err.exists(_.contains("ccas use-club <slug>")))
    },
    test("--clear with nothing set succeeds without writing") {
      for {
        code <- UseClub.run(Nil, clear = true, None, Server)
        out  <- TestConsole.output
      } yield assertTrue(code == ExitCode.success, out == Vector("no current club set\n"))
    },
    // Arity rejection — without it these silently set the first slug and discard the rest.
    test("two slugs are rejected with exit 2, naming both") {
      for {
        code <- UseClub.run(List("team-a", "team-b"), clear = false, None, Server)
        err  <- TestConsole.outputErr
      } yield assertTrue(code == ExitCode(2), err.exists(e => e.contains("team-a") && e.contains("team-b")))
    },
    // The case that used to silently SET the club the user asked to clear: zio-cli swallows an option written after a
    // positional, so `--clear` arrives as a second slug. The message must point at the working ordering.
    test("a flag swallowed as a second positional is rejected with the correct ordering hinted") {
      for {
        code <- UseClub.run(List("team-alpha", "--clear"), clear = false, None, Server)
        err  <- TestConsole.outputErr
      } yield assertTrue(
        code == ExitCode(2),
        err.exists(_.contains("ccas use-club --clear team-alpha"))
      )
    },
    // classify decides the advisory from the live managed-set fetch. `None` = the probe got no usable answer, which
    // must not be reported as "unmanaged" — it falls through to the offline cache hint instead.
    test("classify: a slug in the live managed set is Managed") {
      assertTrue(UseClub.classify("team-alpha", Some(List("team-beta", "team-alpha"))) == UseClub.Verify.Managed)
    },
    test("classify: managed match is case-insensitive") {
      assertTrue(UseClub.classify("Team-Alpha", Some(List("team-alpha"))) == UseClub.Verify.Managed)
    },
    test("classify: a slug absent from a reached managed set is Unmanaged") {
      assertTrue(UseClub.classify("team-gamma", Some(List("team-alpha"))) == UseClub.Verify.Unmanaged)
    },
    test("classify: an empty-but-reached managed set is Unmanaged, not Unverified") {
      assertTrue(UseClub.classify("team-alpha", Some(Nil)) == UseClub.Verify.Unmanaged)
    },
    test("classify: no usable answer from the server is Unverified") {
      assertTrue(UseClub.classify("team-alpha", None) == UseClub.Verify.Unverified)
    },
    // matchedId is what upgrades the pointer to the rename-proof `<id>:<slug>` form. `club.slug` carries no unique
    // index since #254, so the managed set can show one name twice — and then no id is safe to store.
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
    suite("advise")(
      test("Managed says nothing") {
        for {
          _   <- UseClub.advise("team-alpha", UseClub.Verify.Managed)
          err <- TestConsole.outputErr
        } yield assertTrue(err.isEmpty)
      },
      test("Unmanaged names the club and how to manage it") {
        for {
          _   <- UseClub.advise("team-gamma", UseClub.Verify.Unmanaged)
          err <- TestConsole.outputErr
        } yield assertTrue(err.exists(e => e.contains("not one of your managed clubs") && e.contains("ccas club add team-gamma")))
      }
    ),
    // offlineHint's `None` branch is the only consumer of the Option that `readClubs` returns — without it, collapsing
    // that back to a bare list would compile and break nothing.
    suite("offlineHint")(
      test("an unreadable cache is reported, not treated as empty") {
        for {
          _   <- UseClub.offlineHint("team-alpha", None)
          err <- TestConsole.outputErr
        } yield assertTrue(err.exists(_.contains("club cache could not be read")))
      },
      test("a read cache that lacks the slug gives the stale-list hint") {
        for {
          _   <- UseClub.offlineHint("team-gamma", Some(List("team-alpha")))
          err <- TestConsole.outputErr
        } yield assertTrue(err.exists(_.contains("not in the cached club list")))
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
