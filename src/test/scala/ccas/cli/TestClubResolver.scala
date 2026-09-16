package ccas.cli

import zio.{Task, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.ClubQuery
import ccas.api.misc.subtypes.{ClubId, ClubSlug}

/** Tests the pure club-target resolution ([[ClubResolver]]): explicit `--club` / `--club-id` / `--all` wins, then the
  * config's `current_club`, then a usage error (exit 2). Naming a club twice over is always a usage error, never a
  * silent precedence. The `--all` managed-club fetch is injected as a thunk, so a branch that must not fetch is
  * asserted by passing a thunk that fails if forced. No server, no DB.
  */
object TestClubResolver extends ZIOSpecDefault {

  // A fetch thunk that fails if it is ever evaluated — used to prove the non-`--all` branches never touch the network.
  private def mustNotFetch: Task[List[String]] = ZIO.fail(new RuntimeException("managed-club fetch should not run"))

  private def isUsageError(e: Throwable): Boolean = e match {
    case c: CliError => c.exitCode == 2
    case _           => false
  }

  override def spec: Spec[Any, Any] = suite("TestClubResolver")(
    test("single: explicit --club wins over current_club") {
      ClubResolver.single(Some("explicit"), clubIdOption = None, Some("current")).map(s =>
        assertTrue(s.label == "explicit")
      )
    },
    test("single: falls back to current_club when --club is absent") {
      ClubResolver.single(None, clubIdOption = None, Some("current")).map(s =>
        assertTrue(s.label == "current")
      )
    },
    test("single: fails with exit 2 when neither is set") {
      ClubResolver.single(None, clubIdOption = None, None).either.map(r => assertTrue(r.left.exists(_.exitCode == 2)))
    },
    // The label is what every message about this club says, so it reads as the normalised name rather than as typed.
    test("single: trims surrounding whitespace and labels by the normalised slug") {
      for {
        padded <- ClubResolver.single(Some("  team-a  "), clubIdOption = None, None)
        mixed  <- ClubResolver.single(Some("Team-A"), clubIdOption = None, None)
      } yield assertTrue(padded.label == "team-a", mixed.label == "team-a")
    },
    test("single: a whitespace-only --club is treated as unset and falls back to current_club") {
      ClubResolver.single(Some("   "), clubIdOption = None, Some("current")).map(s =>
        assertTrue(s.label == "current")
      )
    },
    // Pins the deliberate non-validation the auto-clear in `Dispatcher` exists to compensate for: an unmanaged (or
    // renamed-away) current_club resolves verbatim, so nothing here catches a club the user has stopped managing.
    test("single: returns an unmanaged current_club verbatim — it validates nothing") {
      ClubResolver.single(None, clubIdOption = None, Some("no-longer-managed")).map(s =>
        assertTrue(s.label == "no-longer-managed")
      )
    },
    test("single: an id:slug current_club carries the stable id (rename-proof target)") {
      ClubResolver.single(None, clubIdOption = None, Some("42:team-a")).map(t =>
        assertTrue(t.query == ClubQuery.ById(ClubId(42L)), t.label == "team-a")
      )
    },
    test("single: an explicit --club carries no id (freshly-typed slug)") {
      ClubResolver.single(Some("team-a"), clubIdOption = None, Some("42:current")).map(t =>
        assertTrue(t.query == ClubQuery.BySlug(ClubSlug(t.label)))
      )
    },
    test("single: a bare-slug current_club carries no id yet") {
      ClubResolver.single(None, clubIdOption = None, Some("team-a")).map(t =>
        assertTrue(t.query == ClubQuery.BySlug(ClubSlug(t.label)), t.label == "team-a")
      )
    },
    test("multi: an id:slug current_club carries the id; --all clubs carry none") {
      for {
        current <- ClubResolver.multi(mustNotFetch, Nil, clubIdOption = None, all = false, Some("7:team-a"))
        all     <- ClubResolver.multi(ZIO.succeed(List("m1")), Nil, clubIdOption = None, all = true, None)
      } yield assertTrue(
        current.head.query == ClubQuery.ById(ClubId(7L)),
        current.head.label == "team-a",
        all.head.query == ClubQuery.BySlug(ClubSlug(all.head.label))
      )
    },
    test("multi: explicit list wins, without fetching managed clubs") {
      ClubResolver.multi(mustNotFetch, List("a", "b"), clubIdOption = None, all = false, Some("current")).map(cs =>
        assertTrue(cs.map(t => t.label).toList == List("a", "b"))
      )
    },
    test("multi: falls back to current_club when no explicit clubs and not --all") {
      ClubResolver.multi(mustNotFetch, Nil, clubIdOption = None, all = false, Some("current")).map(cs =>
        assertTrue(cs.map(t => t.label).toList == List("current"))
      )
    },
    test("multi: trims whitespace and drops blank entries") {
      ClubResolver.multi(mustNotFetch, List(" a ", "", "  ", "b"), clubIdOption = None, all = false, None).map(cs =>
        assertTrue(cs.map(t => t.label).toList == List("a", "b"))
      )
    },
    test("multi: --all expands to the fetched managed clubs") {
      ClubResolver.multi(ZIO.succeed(List("m1", "m2")), Nil, clubIdOption = None, all = true, None).map(cs =>
        assertTrue(cs.map(t => t.label).toList == List("m1", "m2"))
      )
    },
    // The guard fires before the fetch: `mustNotFetch` proves --all+--club is rejected without a network call.
    test("multi: --all together with explicit --club is rejected with exit 2") {
      ClubResolver.multi(mustNotFetch, List("a"), clubIdOption = None, all = true, Some("current")).either.map(r =>
        assertTrue(r.left.exists(e => isUsageError(e) && e.getMessage == ClubResolver.BothError))
      )
    },
    test("multi: --all with no managed clubs fails with exit 2") {
      ClubResolver.multi(ZIO.succeed(Nil), Nil, clubIdOption = None, all = true, Some("ignored")).either.map(r =>
        assertTrue(r.left.exists(isUsageError))
      )
    },
    test("multi: fails with exit 2 when nothing is resolvable") {
      ClubResolver.multi(mustNotFetch, Nil, clubIdOption = None, all = false, None).either.map(r =>
        assertTrue(r.left.exists(isUsageError))
      )
    },
    // `--club-id` names the club outright, which is what settles a name several clubs have held. An id and a name are
    // two answers that can disagree, so passing both is refused rather than one quietly winning (#254).
    test("single: --club-id names the club on its own, labelled by its id, outranking current_club") {
      ClubResolver.single(None, Some(621L), Some("42:current")).map(t =>
        assertTrue(t.query == ClubQuery.ById(ClubId(621L)), t.label == "#621")
      )
    },
    test("single: --club-id together with --club is a usage error") {
      ClubResolver.single(Some("was-shared"), Some(621L), None).either.map(r =>
        assertTrue(r.left.exists(e => e.exitCode == 2 && e.message == ClubResolver.ClubIdError))
      )
    },
    test("multi: --club-id is one target, and never fetches the managed set") {
      ClubResolver.multi(mustNotFetch, Nil, Some(621L), all = false, Some("42:current")).map(cs =>
        assertTrue(cs.head.query == ClubQuery.ById(ClubId(621L)), cs.size == 1)
      )
    },
    test("multi: --club-id with --club or with --all is a usage error, and never fetches") {
      for {
        withClub <- ClubResolver.multi(mustNotFetch, List("a"), Some(621L), all = false, None).either
        withAll  <- ClubResolver.multi(mustNotFetch, Nil, Some(621L), all = true, None).either
      } yield assertTrue(
        withClub.left.exists(e => isUsageError(e) && e.getMessage == ClubResolver.ClubIdError),
        withAll.left.exists(e => isUsageError(e) && e.getMessage == ClubResolver.ClubIdAllError)
      )
    }
  )
}
