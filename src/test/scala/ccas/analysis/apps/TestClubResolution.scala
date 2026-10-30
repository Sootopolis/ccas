package ccas.analysis.apps

import java.time.{Instant, LocalDateTime, ZoneOffset}

import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, fakeChessComClient}
import ccas.analysis.tables.{Club, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.FreshSchemaLayer

/** Resolution of the club a job is addressed to: classify it by id, current name or former name ([[resolveSuite]]),
  * then let Chess.com settle a name local data only knows from history ([[adjudicateSuite]]). `runnable` and the wire
  * round trip are pure.
  */
object TestClubResolution extends ZIOSpecDefault {

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private def club(id: Long, slug: String): Club = Club(ClubId(id), t0, ClubSlug(slug), s"Club $id", None, None, None)

  private val realClub    = club(200, "club-a")
  private val realNamed   = NamedClub(realClub.clubId, realClub.slug)
  private val otherNamed  = NamedClub(ClubId(201), ClubSlug("club-b"))
  private val otherRef    = ClubRef.of(otherNamed)
  private val namelessRef = ClubRef(ClubId(500), None)
  private val byName      = ClubQuery.BySlug(ClubSlug("x"))

  /** The pair every submit runs, as the submit gate calls it. */
  private def adjudicated(client: ChessComClient, query: ClubQuery) =
    ClubResolution.resolveAndAdjudicate(client, query)

  override def spec: Spec[Any, Throwable] = suite("TestClubResolution")(
    suite("against the database")(resolveSuite, adjudicateSuite)
      .provideShared(FreshSchemaLayer("test_club_resolution", onInit = Tables.ensureTables)) @@
      TestAspect.sequential,
    pureSuite
  )

  private def resolveSuite = suite("resolve (local)")(
    test("a real club resolves to Known") {
      for {
        _ <- Club.upsert(realClub)
        v <- ClubResolution.resolve(ClubQuery.BySlug(realClub.slug))
      } yield assertTrue(v == ClubResolution.Known(realNamed))
    },
    test("resolving by id ignores a stale slug and still finds the club (Known)") {
      for {
        _ <- Club.upsert(realClub)
        v <- ClubResolution.resolve(ClubQuery.ById(realClub.clubId))
      } yield assertTrue(v == ClubResolution.Known(realNamed))
    },
    test("an unknown slug is NotLocal (not NotFound — we never asked upstream)") {
      ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("no-such-club"))).map { v =>
        assertTrue(v == ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("no-such-club"))))
      }
    },
    test("an id we have never ingested is NotLocal") {
      val query = ClubQuery.ById(ClubId(999))
      ClubResolution.resolve(query).map(v => assertTrue(v == ClubResolution.NotLocal(query)))
    },
    // #176: before `club_name`, a renamed club did not resolve under its old slug at all.
    test("a former name resolves to the club that held it, as Renamed") {
      val renamed = club(600, "renamed-now")
      for {
        _ <- Club.upsert(renamed.copy(slug = ClubSlug("renamed-before")))
        _ <- Club.upsert(renamed)
        v <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("renamed-before")))
        expected = ClubResolution.Renamed(NamedClub(renamed.clubId, renamed.slug), ClubSlug("renamed-before"))
      } yield assertTrue(v == expected)
    },
    test("the current holder of a name wins over a club that held it before") {
      val before = club(610, "recycled")
      val now    = club(611, "recycled")
      for {
        _ <- Club.upsert(before)
        _ <- Club.upsert(before.copy(slug = ClubSlug("before-moved-on")))
        _ <- Club.upsert(now)
        v <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("recycled")))
      } yield assertTrue(v == ClubResolution.Known(NamedClub(now.clubId, now.slug)))
    },
    test("a name nobody holds now but several clubs held before is Ambiguous") {
      val first  = club(620, "passed-around")
      val second = club(621, "passed-around")
      for {
        _ <- Club.upsert(first)
        _ <- Club.upsert(first.copy(slug = ClubSlug("first-now")))
        _ <- Club.upsert(second)
        _ <- Club.upsert(second.copy(slug = ClubSlug("second-now")))
        v <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("passed-around")))
      } yield assertTrue(
        v == ClubResolution.Ambiguous(
          ClubSlug("passed-around"),
          List(
            ClubRef(first.clubId, Some(ClubSlug("first-now"))),
            ClubRef(second.clubId, Some(ClubSlug("second-now")))
          )
        )
      )
    },
    // 630 renames to new-630, 631 then takes that name, and 630 is left holding none we know of (#254 step 4): it
    // can be named by neither the id nor the name it used to answer to.
    test("a club whose name another club took holds none: Nameless by id, and a NamelessHolder by its former name") {
      for {
        _      <- seedNameless(630, "went-stale")
        byId   <- ClubResolution.resolve(ClubQuery.ById(ClubId(630)))
        bySlug <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("went-stale")))
      } yield assertTrue(
        byId == ClubResolution.Nameless(ClubId(630)),
        bySlug == ClubResolution.NamelessHolder(ClubSlug("went-stale"), ClubId(630))
      )
    }
  )

  // Each test seeds a club that has since changed its name, so the slug it asks about resolves out of local history —
  // the only answers adjudication acts on. `club/<slug>` is 404 unless the fake client is given a body for it.
  private def adjudicateSuite = suite("adjudicate (upstream)")(
    test("a former name nobody holds upstream leaves the local answer standing") {
      val club700 = club(700, "now-700")
      for {
        _           <- Club.upsert(club700.copy(slug = ClubSlug("old-700")))
        _           <- Club.upsert(club700)
        local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("old-700")))
        client      <- fakeChessComClient(Map.empty)
        adjudicated <- ClubResolution.adjudicate(client, local)
        expected = ClubResolution.Renamed(NamedClub(club700.clubId, club700.slug), ClubSlug("old-700"))
      } yield assertTrue(adjudicated == expected)
    },
    test("a former name its own club still holds upstream is Known, and the local record is repaired") {
      val club710 = club(710, "now-710")
      for {
        _           <- Club.upsert(club710.copy(slug = ClubSlug("old-710")))
        _           <- Club.upsert(club710)
        local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("old-710")))
        client      <- fakeChessComClient(Map("club/old-710" -> apiClubJson(710, "old-710")))
        adjudicated <- ClubResolution.adjudicate(client, local)
        repaired    <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("old-710")))
      } yield assertTrue(
        adjudicated == ClubResolution.Known(NamedClub(ClubId(710), ClubSlug("old-710"))),
        repaired == ClubResolution.Known(NamedClub(ClubId(710), ClubSlug("old-710")))
      )
    },
    // Chess.com can answer a request under a name it has normalised away, which is still a former name to whoever
    // typed it — so the note the CLI prints for a former name must survive the upstream check.
    test("a former name answered under the club's canonical slug stays Renamed, not Known") {
      val club780 = club(780, "now-780")
      for {
        _           <- Club.upsert(club780.copy(slug = ClubSlug("old-780")))
        _           <- Club.upsert(club780)
        local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("old-780")))
        client      <- fakeChessComClient(Map("club/old-780" -> apiClubJson(780, "now-780")))
        adjudicated <- ClubResolution.adjudicate(client, local)
        expected = ClubResolution.Renamed(NamedClub(club780.clubId, club780.slug), ClubSlug("old-780"))
      } yield assertTrue(adjudicated == expected)
    },
    test("a former name another club holds upstream runs against that club, persisted, as Moved") {
      val club720 = club(720, "now-720")
      for {
        _           <- Club.upsert(club720.copy(slug = ClubSlug("old-720")))
        _           <- Club.upsert(club720)
        local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("old-720")))
        client      <- fakeChessComClient(Map("club/old-720" -> apiClubJson(729, "old-720")))
        adjudicated <- ClubResolution.adjudicate(client, local)
        persisted   <- Club.selectId(ClubId(729))
      } yield assertTrue(
        adjudicated == ClubResolution.Moved(
          club = NamedClub(ClubId(729), ClubSlug("old-720")),
          requested = ClubSlug("old-720"),
          previous = List(ClubRef(club720.clubId, Some(club720.slug)))
        ),
        persisted.exists(_.slug == ClubSlug("old-720"))
      )
    },
    test("an ambiguous name one of its former holders holds upstream is settled as Known") {
      for {
        _           <- seedContested(730, 731, "shared-730")
        local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("shared-730")))
        client      <- fakeChessComClient(Map("club/shared-730" -> apiClubJson(731, "shared-730")))
        adjudicated <- ClubResolution.adjudicate(client, local)
      } yield assertTrue(
        local.isInstanceOf[ClubResolution.Ambiguous],
        adjudicated == ClubResolution.Known(NamedClub(ClubId(731), ClubSlug("shared-730")))
      )
    },
    test("an ambiguous name a third club holds upstream is Moved, naming every former holder") {
      for {
        _           <- seedContested(740, 741, "shared-740")
        local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("shared-740")))
        client      <- fakeChessComClient(Map("club/shared-740" -> apiClubJson(749, "shared-740")))
        adjudicated <- ClubResolution.adjudicate(client, local)
      } yield assertTrue(
        adjudicated == ClubResolution.Moved(
          club = NamedClub(ClubId(749), ClubSlug("shared-740")),
          requested = ClubSlug("shared-740"),
          previous = List(
            ClubRef(ClubId(740), Some(ClubSlug("was-740"))),
            ClubRef(ClubId(741), Some(ClubSlug("was-741")))
          )
        )
      )
    },
    test("an ambiguous name nobody holds upstream stays ambiguous — this is what the prompt is for") {
      for {
        _           <- seedContested(750, 751, "shared-750")
        local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("shared-750")))
        client      <- fakeChessComClient(Map.empty)
        adjudicated <- ClubResolution.adjudicate(client, local)
      } yield assertTrue(adjudicated == local)
    },
    // The fake client answers these slugs with a *different* club, so an unchanged answer is proof no request fired.
    test("a current name and an unknown name are never adjudicated") {
      val club760 = club(760, "current-760")
      val responses = Map(
        "club/current-760" -> apiClubJson(769, "current-760"),
        "club/never-seen"  -> apiClubJson(769, "never-seen")
      )
      for {
        _        <- Club.upsert(club760)
        client   <- fakeChessComClient(responses)
        known    <- adjudicated(client, ClubQuery.BySlug(club760.slug))
        notLocal <- adjudicated(client, ClubQuery.BySlug(ClubSlug("never-seen")))
      } yield assertTrue(
        known == ClubResolution.Known(NamedClub(club760.clubId, club760.slug)),
        notLocal == ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("never-seen")))
      )
    },
    test("an upstream answer we cannot read degrades to the local answer rather than failing the submit") {
      val club770 = club(770, "now-770")
      for {
        _           <- Club.upsert(club770.copy(slug = ClubSlug("old-770")))
        _           <- Club.upsert(club770)
        local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("old-770")))
        client      <- fakeChessComClient(Map("club/old-770" -> """{"not":"a club"}"""))
        adjudicated <- ClubResolution.adjudicate(client, local)
      } yield assertTrue(adjudicated == local)
    },
    // #274: the one club that held the name lost its own to another club, so local data can name nobody.
    suite("a former name whose only holder holds none")(
      test("held by another club upstream, is Moved, naming the nameless holder, and the new holder is persisted") {
        for {
          _           <- seedNameless(790, "gone-790")
          local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("gone-790")))
          client      <- fakeChessComClient(Map("club/gone-790" -> apiClubJson(799, "gone-790")))
          adjudicated <- ClubResolution.adjudicate(client, local)
          persisted   <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("gone-790")))
        } yield assertTrue(
          adjudicated == ClubResolution.Moved(
            club = NamedClub(ClubId(799), ClubSlug("gone-790")),
            requested = ClubSlug("gone-790"),
            previous = List(ClubRef(ClubId(790), None))
          ),
          persisted == ClubResolution.Known(NamedClub(ClubId(799), ClubSlug("gone-790")))
        )
      },
      test("held by that club again upstream, is Known, and the club answers to its id again") {
        for {
          _           <- seedNameless(800, "gone-800")
          local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("gone-800")))
          client      <- fakeChessComClient(Map("club/gone-800" -> apiClubJson(800, "gone-800")))
          adjudicated <- ClubResolution.adjudicate(client, local)
          byId        <- ClubResolution.resolve(ClubQuery.ById(ClubId(800)))
        } yield assertTrue(
          adjudicated == ClubResolution.Known(NamedClub(ClubId(800), ClubSlug("gone-800"))),
          byId == ClubResolution.Known(NamedClub(ClubId(800), ClubSlug("gone-800")))
        )
      },
      test("held by nobody upstream, stays a NamelessHolder") {
        for {
          _           <- seedNameless(810, "gone-810")
          local       <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("gone-810")))
          client      <- fakeChessComClient(Map.empty)
          adjudicated <- ClubResolution.adjudicate(client, local)
        } yield assertTrue(adjudicated == ClubResolution.NamelessHolder(ClubSlug("gone-810"), ClubId(810)))
      }
    ),
    // The fake client answers the club's former name with another club, so an unchanged answer is proof none was asked.
    test("a nameless club named by its id is never adjudicated, though Chess.com would answer its former name") {
      for {
        _      <- seedNameless(820, "gone-820")
        client <- fakeChessComClient(Map("club/gone-820" -> apiClubJson(829, "gone-820")))
        byId   <- adjudicated(client, ClubQuery.ById(ClubId(820)))
      } yield assertTrue(byId == ClubResolution.Nameless(ClubId(820)))
    }
  ) @@ TestAspect.withLiveClock

  /** Two clubs that both held `slug` and have since moved on, so it resolves [[ClubResolution.Ambiguous]]. */
  private def seedContested(first: Long, second: Long, slug: String) =
    for {
      _ <- Club.upsert(club(first, slug))
      _ <- Club.upsert(club(first, s"was-$first"))
      _ <- Club.upsert(club(second, slug))
      _ <- Club.upsert(club(second, s"was-$second"))
    } yield ()

  /** A club that held `slug`, then took `new-<id>` and lost it to club `id + 1`, so it holds no name we know of. */
  private def seedNameless(id: Long, slug: String) =
    for {
      _ <- Club.upsert(club(id, slug))
      _ <- Club.upsert(club(id, s"new-$id"))
      _ <- Club.upsert(club(id + 1, s"new-$id"))
    } yield ()

  private def pureSuite = suite("runnable / wire")(
    test("Known runs its job") {
      assertTrue(ClubResolution.Known(realNamed).runnable == Right(realNamed))
    },
    test("NotLocal does not run, with a 'Club not found' reason") {
      val notLocal = ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("x")))
      assertTrue(notLocal.runnable.left.exists(_.startsWith("Club not found")))
    },
    test("Renamed runs its job against the current club") {
      assertTrue(ClubResolution.Renamed(realNamed, ClubSlug("old-name")).runnable == Right(realNamed))
    },
    test("Moved runs its job against the club that holds the name now") {
      val moved = ClubResolution.Moved(realNamed, ClubSlug("changed-hands"), List(otherRef))
      assertTrue(moved.runnable == Right(realNamed))
    },
    test("Nameless does not run, and its reason names the club by id") {
      assertTrue(ClubResolution.Nameless(ClubId(630)).runnable.left.exists(_.startsWith("Club #630 can't be reached")))
    },
    test("NamelessHolder does not run, and its reason names the name asked for and the club that held it") {
      val resolution = ClubResolution.NamelessHolder(ClubSlug("went-stale"), ClubId(630))
      assertTrue(
        resolution.runnable.left.exists(m => m.startsWith("Club went-stale can't be reached") && m.contains("#630"))
      )
    },
    test("Ambiguous does not run, and its reason names every candidate by id, one holding no name without one") {
      val resolution = ClubResolution.Ambiguous(ClubSlug("shared"), List(ClubRef.of(realNamed), namelessRef))
      assertTrue(
        resolution.runnable.left.exists(m => m.contains("#200 (now club-a)") && m.contains("#500 (no known name)"))
      )
    },
    test("every case survives the JSON round trip, discriminated by a snake_case kind") {
      val all: List[ClubResolution] = List(
        ClubResolution.Known(realNamed),
        ClubResolution.Renamed(realNamed, ClubSlug("old-name")),
        ClubResolution.NotLocal(byName),
        ClubResolution.Nameless(ClubId(9)),
        ClubResolution.NamelessHolder(ClubSlug("y"), ClubId(9)),
        ClubResolution.Ambiguous(ClubSlug("z"), List(ClubRef.of(realNamed), namelessRef)),
        ClubResolution.Moved(realNamed, ClubSlug("changed-hands"), List(otherRef))
      )
      assertTrue(
        all.forall(resolution => resolution.toJson.fromJson[ClubResolution] == Right(resolution)),
        (ClubResolution.NotLocal(byName): ClubResolution).toJson.contains("\"kind\":\"not_local\"")
      )
    }
  )
}
