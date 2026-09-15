package ccas.analysis.apps

import java.time.{Instant, LocalDateTime, ZoneOffset}

import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.sql.FreshSchemaLayer

/** Local-reach resolution: classify a club by id, current name or former name into a [[ClubResolution]]. DB-backed for
  * the `resolve` cases; `runnable` and the wire round trip are pure.
  */
object TestClubResolution extends ZIOSpecDefault {

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private def club(id: Long, slug: String): Club = Club(ClubId(id), t0, ClubSlug(slug), s"Club $id", None, None, None)

  private val realClub    = club(200, "club-a")
  private val tombstoned  = club(500, "_stale_500")
  private val realRef     = ClubRef.fromClub(realClub)

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
      } yield assertTrue(v == ClubResolution.Known(realRef))
    },
    test("resolving by id ignores a stale slug and still finds the club (Known)") {
      for {
        _ <- Club.upsert(realClub)
        v <- ClubResolution.resolve(Some(realClub.clubId), ClubSlug("was-renamed"))
      } yield assertTrue(v == ClubResolution.Known(realRef))
    },
    test("an unknown slug is NotLocal (not NotFound — we never asked upstream)") {
      ClubResolution.resolve(None, ClubSlug("no-such-club")).map(v =>
        assertTrue(v == ClubResolution.NotLocal(ClubSlug("no-such-club")))
      )
    },
    test("a tombstoned club resolved by id is Problematic, carrying the requested slug not the _stale_ placeholder") {
      for {
        _ <- Club.upsert(tombstoned)
        v <- ClubResolution.resolve(Some(tombstoned.clubId), ClubSlug("team-old"))
      } yield assertTrue(v == ClubResolution.Problematic(ClubSlug("team-old")))
    },
    // #176: before `club_name`, a renamed club did not resolve under its old slug at all.
    test("a former name resolves to the club that held it, as Renamed") {
      val renamed = club(600, "renamed-now")
      for {
        _ <- Club.upsert(renamed.copy(slug = ClubSlug("renamed-before")))
        _ <- Club.upsert(renamed)
        v <- ClubResolution.resolve(None, ClubSlug("renamed-before"))
      } yield assertTrue(v == ClubResolution.Renamed(ClubRef.fromClub(renamed), ClubSlug("renamed-before")))
    },
    test("the current holder of a name wins over a club that held it before") {
      val before = club(610, "recycled")
      val now    = club(611, "recycled")
      for {
        _ <- Club.upsert(before)
        _ <- Club.upsert(before.copy(slug = ClubSlug("before-moved-on")))
        _ <- Club.upsert(now)
        v <- ClubResolution.resolve(None, ClubSlug("recycled"))
      } yield assertTrue(v == ClubResolution.Known(ClubRef.fromClub(now)))
    },
    test("a name nobody holds now but several clubs held before is Ambiguous") {
      val first  = club(620, "passed-around")
      val second = club(621, "passed-around")
      for {
        _ <- Club.upsert(first)
        _ <- Club.upsert(first.copy(slug = ClubSlug("first-now")))
        _ <- Club.upsert(second)
        _ <- Club.upsert(second.copy(slug = ClubSlug("second-now")))
        v <- ClubResolution.resolve(None, ClubSlug("passed-around"))
      } yield assertTrue(
        v == ClubResolution.Ambiguous(
          ClubSlug("passed-around"),
          List(ClubRef(first.clubId, ClubSlug("first-now")), ClubRef(second.clubId, ClubSlug("second-now")))
        )
      )
    },
    test("a former name whose only holder is now tombstoned is Problematic") {
      val gone = club(630, "went-stale")
      for {
        _ <- Club.upsert(gone)
        _ <- Club.upsert(gone.copy(slug = ClubSlug("_stale_630")))
        v <- ClubResolution.resolve(None, ClubSlug("went-stale"))
      } yield assertTrue(v == ClubResolution.Problematic(ClubSlug("went-stale")))
    }
  )

  private def pureSuite = suite("runnable / wire")(
    test("Known runs its job") {
      assertTrue(ClubResolution.Known(realRef).runnable == Right(realRef))
    },
    test("NotLocal does not run, with a 'Club not found' reason") {
      assertTrue(ClubResolution.NotLocal(ClubSlug("x")).runnable.left.exists(_.startsWith("Club not found")))
    },
    test("Renamed runs its job against the current club") {
      assertTrue(ClubResolution.Renamed(realRef, ClubSlug("old-name")).runnable == Right(realRef))
    },
    test("Ambiguous does not run, and its reason names every candidate by id, a tombstoned one without a name") {
      val resolution = ClubResolution.Ambiguous(ClubSlug("shared"), List(realRef, ClubRef.fromClub(tombstoned)))
      assertTrue(
        resolution.runnable.left.exists(m => m.contains("#200 (now club-a)") && m.contains("#500 (no known name)"))
      )
    },
    test("every case survives the JSON round trip, discriminated by a snake_case kind") {
      val all: List[ClubResolution] = List(
        ClubResolution.Known(realRef),
        ClubResolution.Renamed(realRef, ClubSlug("old-name")),
        ClubResolution.NotLocal(ClubSlug("x")),
        ClubResolution.Problematic(ClubSlug("y")),
        ClubResolution.Ambiguous(ClubSlug("z"), List(realRef, ClubRef.fromClub(tombstoned)))
      )
      assertTrue(
        all.forall(resolution => resolution.toJson.fromJson[ClubResolution] == Right(resolution)),
        (ClubResolution.NotLocal(ClubSlug("x")): ClubResolution).toJson.contains("\"kind\":\"not_local\"")
      )
    }
  )
}
