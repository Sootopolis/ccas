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

  private val realClub   = club(200, "club-a")
  private val tombstoned = club(500, "_stale_500")
  private val realRef    = ClubRef.fromClub(realClub)
  private val otherRef   = ClubRef(ClubId(201), ClubSlug("club-b"))
  private val byName     = ClubQuery.BySlug(ClubSlug("x"))

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
      } yield assertTrue(v == ClubResolution.Known(realRef))
    },
    test("resolving by id ignores a stale slug and still finds the club (Known)") {
      for {
        _ <- Club.upsert(realClub)
        v <- ClubResolution.resolve(ClubQuery.ById(realClub.clubId))
      } yield assertTrue(v == ClubResolution.Known(realRef))
    },
    test("an unknown slug is NotLocal (not NotFound — we never asked upstream)") {
      ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("no-such-club"))).map(v =>
        assertTrue(v == ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("no-such-club"))))
      )
    },
    test("a tombstoned club is Problematic, named as it was asked for rather than by its _stale_ placeholder") {
      for {
        _     <- Club.upsert(tombstoned)
        byId  <- ClubResolution.resolve(ClubQuery.ById(tombstoned.clubId))
        _     <- Club.upsert(tombstoned.copy(slug = ClubSlug("team-old")))
        _     <- Club.upsert(tombstoned)
        bySlug <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("team-old")))
      } yield assertTrue(
        byId == ClubResolution.Problematic(ClubQuery.ById(tombstoned.clubId)),
        bySlug == ClubResolution.Problematic(ClubQuery.BySlug(ClubSlug("team-old")))
      )
    },
    // #176: before `club_name`, a renamed club did not resolve under its old slug at all.
    test("a former name resolves to the club that held it, as Renamed") {
      val renamed = club(600, "renamed-now")
      for {
        _ <- Club.upsert(renamed.copy(slug = ClubSlug("renamed-before")))
        _ <- Club.upsert(renamed)
        v <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("renamed-before")))
      } yield assertTrue(v == ClubResolution.Renamed(ClubRef.fromClub(renamed), ClubSlug("renamed-before")))
    },
    test("the current holder of a name wins over a club that held it before") {
      val before = club(610, "recycled")
      val now    = club(611, "recycled")
      for {
        _ <- Club.upsert(before)
        _ <- Club.upsert(before.copy(slug = ClubSlug("before-moved-on")))
        _ <- Club.upsert(now)
        v <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("recycled")))
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
        v <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("passed-around")))
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
        v <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("went-stale")))
      } yield assertTrue(v == ClubResolution.Problematic(ClubQuery.BySlug(ClubSlug("went-stale"))))
    }
  )

  // Each test seeds a club that has since changed its name, so the slug it asks about resolves out of local history —
  // the only answers adjudication acts on. `club/<slug>` is 404 unless the fake client is given a body for it.
  private def adjudicateSuite = suite("adjudicate (upstream)")(
    test("a former name nobody holds upstream leaves the local answer standing") {
      val club700 = club(700, "now-700")
      for {
        _          <- Club.upsert(club700.copy(slug = ClubSlug("old-700")))
        _          <- Club.upsert(club700)
        local      <- ClubResolution.resolve(ClubQuery.BySlug(ClubSlug("old-700")))
        client     <- fakeChessComClient(Map.empty)
        adjudicated <- ClubResolution.adjudicate(client, local)
      } yield assertTrue(adjudicated == ClubResolution.Renamed(ClubRef.fromClub(club700), ClubSlug("old-700")))
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
        adjudicated == ClubResolution.Known(ClubRef(ClubId(710), ClubSlug("old-710"))),
        repaired == ClubResolution.Known(ClubRef(ClubId(710), ClubSlug("old-710")))
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
      } yield assertTrue(adjudicated == ClubResolution.Renamed(ClubRef.fromClub(club780), ClubSlug("old-780")))
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
          club = ClubRef(ClubId(729), ClubSlug("old-720")),
          requested = ClubSlug("old-720"),
          previous = List(ClubRef.fromClub(club720))
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
        adjudicated == ClubResolution.Known(ClubRef(ClubId(731), ClubSlug("shared-730")))
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
          club = ClubRef(ClubId(749), ClubSlug("shared-740")),
          requested = ClubSlug("shared-740"),
          previous = List(ClubRef(ClubId(740), ClubSlug("was-740")), ClubRef(ClubId(741), ClubSlug("was-741")))
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
        _         <- Club.upsert(club760)
        client    <- fakeChessComClient(responses)
        known    <- adjudicated(client, ClubQuery.BySlug(club760.slug))
        notLocal <- adjudicated(client, ClubQuery.BySlug(ClubSlug("never-seen")))
      } yield assertTrue(
        known == ClubResolution.Known(ClubRef.fromClub(club760)),
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

  private def pureSuite = suite("runnable / wire")(
    test("Known runs its job") {
      assertTrue(ClubResolution.Known(realRef).runnable == Right(realRef))
    },
    test("NotLocal does not run, with a 'Club not found' reason") {
      val notLocal = ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("x")))
      assertTrue(notLocal.runnable.left.exists(_.startsWith("Club not found")))
    },
    test("Renamed runs its job against the current club") {
      assertTrue(ClubResolution.Renamed(realRef, ClubSlug("old-name")).runnable == Right(realRef))
    },
    test("Moved runs its job against the club that holds the name now") {
      assertTrue(ClubResolution.Moved(realRef, ClubSlug("changed-hands"), List(otherRef)).runnable == Right(realRef))
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
        ClubResolution.NotLocal(byName),
        ClubResolution.Problematic(ClubQuery.BySlug(ClubSlug("y"))),
        ClubResolution.Ambiguous(ClubSlug("z"), List(realRef, ClubRef.fromClub(tombstoned))),
        ClubResolution.Moved(realRef, ClubSlug("changed-hands"), List(otherRef))
      )
      assertTrue(
        all.forall(resolution => resolution.toJson.fromJson[ClubResolution] == Right(resolution)),
        (ClubResolution.NotLocal(byName): ClubResolution).toJson.contains("\"kind\":\"not_local\"")
      )
    }
  )
}
