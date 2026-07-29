package ccas.analysis.apps

import java.time.{Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.errors.ClubProblem
import ccas.utils.sql.FreshSchemaLayer

/** Local-reach resolution: classify a club by id-or-slug into a [[ClubVerdict]], and map non-`Known` verdicts to their
  * wire [[ClubProblem]]. DB-backed for the `resolve` cases; the `toProblem`/`message` mappings are pure.
  */
object TestClubResolution extends ZIOSpecDefault {

  private val t0: Instant                = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
  private val realClub                   = Club(ClubId(200), t0, ClubSlug("club-a"), "Club A", None, None, None)
  private val tombstoned                 = Club(ClubId(500), t0, ClubSlug("_stale_500"), "Gone", None, None, None)

  override def spec: Spec[Any, Throwable] = suite("TestClubResolution")(
    resolveSuite.provideShared(FreshSchemaLayer("test_club_resolution", onInit = Tables.ensureTables)) @@
      TestAspect.sequential,
    pureSuite
  )

  private def resolveSuite = suite("resolve (local)")(
    test("a real club resolves to Known") {
      for {
        _ <- Club.upsert(realClub)
        v <- ClubResolution.resolve(None, realClub.slug)
      } yield assertTrue(v == ClubVerdict.Known(realClub))
    },
    test("resolving by id ignores a stale slug and still finds the club (Known)") {
      for {
        _ <- Club.upsert(realClub)
        v <- ClubResolution.resolve(Some(realClub.clubId), ClubSlug("was-renamed"))
      } yield assertTrue(v == ClubVerdict.Known(realClub))
    },
    test("an unknown slug is NotLocal (not NotFound — we never asked upstream)") {
      ClubResolution.resolve(None, ClubSlug("no-such-club")).map(v =>
        assertTrue(v == ClubVerdict.NotLocal(ClubSlug("no-such-club")))
      )
    },
    test("a tombstoned club resolved by id is Problematic, carrying the requested slug not the _stale_ placeholder") {
      for {
        _ <- Club.upsert(tombstoned)
        v <- ClubResolution.resolve(Some(tombstoned.clubId), ClubSlug("team-old"))
      } yield assertTrue(v == ClubVerdict.Problematic(ClubSlug("team-old")))
    }
  )

  private def pureSuite = suite("toProblem / message")(
    test("Known carries no problem or message") {
      val v = ClubVerdict.Known(realClub)
      assertTrue(v.problem.isEmpty, v.message.isEmpty)
    },
    test("NotLocal maps to NotFound with a 'Club not found' message") {
      val v = ClubVerdict.NotLocal(ClubSlug("x"))
      assertTrue(
        v.problem.contains(ClubProblem.NotFound),
        v.message.exists(_.startsWith("Club not found"))
      )
    },
    test("Problematic maps to the Problematic wire arm") {
      val v = ClubVerdict.Problematic(ClubSlug("_stale_9"))
      assertTrue(v.problem.contains(ClubProblem.Problematic))
    }
  )
}
