package ccas.cli

import zio.{Task, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.subtypes.ClubSlug

/** Tests the pure club-target resolution ([[ClubResolver]]): explicit `--club` / `--all` wins, then the config's
  * `current_club`, then a usage error (exit 2). The `--all` managed-club fetch is injected as a thunk, so a branch that
  * must not fetch is asserted by passing a thunk that fails if forced. No server, no DB.
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
      ClubResolver.single(Some("explicit"), Some("current")).map(s => assertTrue(ClubSlug.unwrap(s) == "explicit"))
    },
    test("single: falls back to current_club when --club is absent") {
      ClubResolver.single(None, Some("current")).map(s => assertTrue(ClubSlug.unwrap(s) == "current"))
    },
    test("single: fails with exit 2 when neither is set") {
      ClubResolver.single(None, None).either.map(r => assertTrue(r.left.exists(_.exitCode == 2)))
    },
    test("single: trims surrounding whitespace from the slug") {
      ClubResolver.single(Some("  team-a  "), None).map(s => assertTrue(ClubSlug.unwrap(s) == "team-a"))
    },
    test("single: a whitespace-only --club is treated as unset and falls back to current_club") {
      ClubResolver.single(Some("   "), Some("current")).map(s => assertTrue(ClubSlug.unwrap(s) == "current"))
    },
    test("multi: explicit list wins, without fetching managed clubs") {
      ClubResolver.multi(mustNotFetch, List("a", "b"), all = false, Some("current")).map(cs =>
        assertTrue(cs.map(ClubSlug.unwrap).toList == List("a", "b"))
      )
    },
    test("multi: falls back to current_club when no explicit clubs and not --all") {
      ClubResolver.multi(mustNotFetch, Nil, all = false, Some("current")).map(cs =>
        assertTrue(cs.map(ClubSlug.unwrap).toList == List("current"))
      )
    },
    test("multi: trims whitespace and drops blank entries") {
      ClubResolver.multi(mustNotFetch, List(" a ", "", "  ", "b"), all = false, None).map(cs =>
        assertTrue(cs.map(ClubSlug.unwrap).toList == List("a", "b"))
      )
    },
    test("multi: --all expands to the fetched managed clubs") {
      ClubResolver.multi(ZIO.succeed(List("m1", "m2")), Nil, all = true, None).map(cs =>
        assertTrue(cs.map(ClubSlug.unwrap).toList == List("m1", "m2"))
      )
    },
    // The guard fires before the fetch: `mustNotFetch` proves --all+--club is rejected without a network call.
    test("multi: --all together with explicit --club is rejected with exit 2") {
      ClubResolver.multi(mustNotFetch, List("a"), all = true, Some("current")).either.map(r =>
        assertTrue(r.left.exists(e => isUsageError(e) && e.getMessage == ClubResolver.BothError))
      )
    },
    test("multi: --all with no managed clubs fails with exit 2") {
      ClubResolver.multi(ZIO.succeed(Nil), Nil, all = true, Some("ignored")).either.map(r =>
        assertTrue(r.left.exists(isUsageError))
      )
    },
    test("multi: fails with exit 2 when nothing is resolvable") {
      ClubResolver.multi(mustNotFetch, Nil, all = false, None).either.map(r => assertTrue(r.left.exists(isUsageError)))
    }
  )
}
