package ccas.analysis.apps

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.RIO

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{
  apiClubJson,
  apiPlayerClubsJson,
  fakeChessComClient
}
import ccas.analysis.tables.{Club, ClubAdmin, Player, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient, TestDbCleanup}
import ccas.utils.sql.PostgresClient.connectZIO

object TestClubSlugRenameResolver extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = (suite("TestClubSlugRenameResolver")(
    suite("resolveOrFetch")(
      happyPathLocalHit,
      coldDiscoveryFetchesAndPersists,
      fourOhFourReturnsNone
    ),
    suite("resolveAndPersist tier C (admin-clubs lookup)")(
      tierCHitFromAdminClubs,
      tierCMissAllAdminsChurned,
      tierCEmptyAdminListReturnsNone,
      tierCSkipsTombstonedAdminUsername,
      tierCSkipsClosedAdminUsername,
      tierCSkipsSlugsAlreadyKnownToDb,
      tierCAdvancesPastAdminWhose404s,
      tierCNoOpWithoutHint
    )
  ) @@ TestAspect.before(resetTables)).provideShared(
    FreshSchemaLayer("test_club_slug_rename_resolver", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private val resetTables: RIO[PostgresClient, Unit] =
    TestDbCleanup.clearApiCache *> TestDbCleanup.clearClub *> TestDbCleanup.clearPlayer

  private val t0 = Instant.parse("2025-01-01T00:00:00Z")

  private def insertPlayer(pid: PlayerId, username: String): RIO[PostgresClient, Unit] =
    Player.insert(Player(pid, t0, Username(username), PlayerStatusCategory.Active, None, t0))

  private def insertClosedPlayer(pid: PlayerId, username: String): RIO[PostgresClient, Unit] =
    Player.insert(Player(pid, t0, Username(username), PlayerStatusCategory.Closed, None, t0))

  private def insertAdmin(clubId: ClubId, playerId: PlayerId): RIO[PostgresClient, Unit] =
    ClubAdmin.insertBatch(List(ClubAdmin(clubId, playerId))).unit

  private def fetchFailureCountFor(urlSubstring: String): RIO[PostgresClient, Long] =
    connectZIO {
      sql"SELECT COUNT(*) FROM api_fetch_failure WHERE url LIKE ${"%" + urlSubstring + "%"}"
        .query[Long].run().head
    }

  // --- resolveOrFetch suite ---

  private def happyPathLocalHit = test("local hit: returns clubId without HTTP") {
    val clubId = ClubId(901_001)
    val slug   = ClubSlug("locally-known")
    for {
      _ <- Club.upsert(Club(clubId, t0, slug, "Local", None, None, None))
      // No HTTP routes registered — any API call would 404.
      client <- fakeChessComClient(Map.empty)
      result <- ClubSlugRenameResolver.resolveOrFetch(client, slug)
    } yield assertTrue(result.contains(clubId))
  }

  private def coldDiscoveryFetchesAndPersists = test("cold discovery: fetches, persists, returns clubId") {
    val clubId = ClubId(901_002)
    val slug   = ClubSlug("first-time")
    val responses = Map(s"club/${slug.value}" -> apiClubJson(ClubId.unwrap(clubId), slug.value))
    for {
      client    <- fakeChessComClient(responses)
      result    <- ClubSlugRenameResolver.resolveOrFetch(client, slug)
      persisted <- Club.selectBySlug(slug)
    } yield assertTrue(result.contains(clubId), persisted.exists(_.clubId == clubId))
  }

  private def fourOhFourReturnsNone = test("404 + no Club row: returns None (recovery not wired through this entry point)") {
    val slug = ClubSlug("never-existed")
    for {
      client <- fakeChessComClient(Map.empty)
      result <- ClubSlugRenameResolver.resolveOrFetch(client, slug)
    } yield assertTrue(result.isEmpty)
  }

  // --- Tier C suite ---

  private def tierCHitFromAdminClubs =
    test("Tier C: admin's club list yields a slug whose ApiClub.clubId matches the hint → returns fresh slug") {
      val clubId    = ClubId(902_001)
      val staleSlug = ClubSlug("tier-c-stale")
      val freshSlug = ClubSlug("tier-c-fresh")
      val adminPid  = PlayerId(902_101)
      val adminName = "tier-c-admin"
      val responses = Map(
        s"player/$adminName/clubs"  -> apiPlayerClubsJson(List(freshSlug.value)),
        s"club/${freshSlug.value}"  -> apiClubJson(ClubId.unwrap(clubId), freshSlug.value)
      )
      for {
        _      <- Club.upsert(Club(clubId, t0, staleSlug, "Old Name", None, None, None))
        _      <- insertPlayer(adminPid, adminName)
        _      <- insertAdmin(clubId, adminPid)
        client <- fakeChessComClient(responses)
        result <- ClubSlugRenameResolver.resolveAndPersist(client, staleSlug, Some(clubId))
        updated <- Club.selectId(clubId)
      } yield assertTrue(
        result.exists((slug, ac) => slug == freshSlug && ac.clubId == clubId),
        updated.exists(_.slug == freshSlug)
      )
    }

  private def tierCMissAllAdminsChurned =
    test("Tier C: no admin's clubs list contains a slug matching the hint → None") {
      val clubId    = ClubId(902_002)
      val staleSlug = ClubSlug("tier-c-no-match")
      val otherSlug = ClubSlug("unrelated-club")
      val adminPid  = PlayerId(902_102)
      val adminName = "lonely-admin"
      // Admin belongs to `unrelated-club` whose ApiClub returns a different clubId → no match.
      val responses = Map(
        s"player/$adminName/clubs" -> apiPlayerClubsJson(List(otherSlug.value)),
        s"club/${otherSlug.value}" -> apiClubJson(ClubId.unwrap(ClubId(999_999)), otherSlug.value)
      )
      for {
        _      <- Club.upsert(Club(clubId, t0, staleSlug, "Stale", None, None, None))
        _      <- insertPlayer(adminPid, adminName)
        _      <- insertAdmin(clubId, adminPid)
        client <- fakeChessComClient(responses)
        result <- ClubSlugRenameResolver.resolveAndPersist(client, staleSlug, Some(clubId))
      } yield assertTrue(result.isEmpty)
    }

  private def tierCEmptyAdminListReturnsNone =
    test("Tier C: no ClubAdmin rows for the club → None (no HTTP fan-out)") {
      val clubId    = ClubId(902_003)
      val staleSlug = ClubSlug("tier-c-empty-admins")
      // No routes — any HTTP call would 404, proving the tier didn't fan out.
      for {
        _      <- Club.upsert(Club(clubId, t0, staleSlug, "No Admins", None, None, None))
        client <- fakeChessComClient(Map.empty)
        result <- ClubSlugRenameResolver.resolveAndPersist(client, staleSlug, Some(clubId))
      } yield assertTrue(result.isEmpty)
    }

  private def tierCSkipsTombstonedAdminUsername =
    test("Tier C: tombstoned admin row is skipped without an HTTP call; another admin still resolves") {
      val clubId        = ClubId(902_004)
      val staleSlug     = ClubSlug("tier-c-tombstone")
      val freshSlug     = ClubSlug("tier-c-tombstone-fresh")
      val tombstonePid  = PlayerId(902_201)
      val livePid       = PlayerId(902_202)
      val livename      = "live-admin"
      val tombstoneName = s"_stale_${PlayerId.unwrap(tombstonePid)}"
      val responses = Map(
        s"player/$livename/clubs"  -> apiPlayerClubsJson(List(freshSlug.value)),
        s"club/${freshSlug.value}" -> apiClubJson(ClubId.unwrap(clubId), freshSlug.value)
      )
      for {
        _      <- Club.upsert(Club(clubId, t0, staleSlug, "TS Test", None, None, None))
        _      <- insertPlayer(tombstonePid, tombstoneName)
        _      <- insertPlayer(livePid, livename)
        _      <- insertAdmin(clubId, tombstonePid)
        _      <- insertAdmin(clubId, livePid)
        client <- fakeChessComClient(responses)
        result <- ClubSlugRenameResolver.resolveAndPersist(client, staleSlug, Some(clubId))
        // No `api_fetch_failure` row should reference the tombstone URL — proves Tier C never hit it.
        tombstoneFailures <- fetchFailureCountFor(s"player/$tombstoneName")
      } yield assertTrue(
        result.exists((s, _) => s == freshSlug),
        tombstoneFailures == 0L
      )
    }

  private def tierCSkipsClosedAdminUsername =
    test("Tier C: closed admin row is skipped without an HTTP call; another admin still resolves") {
      val clubId     = ClubId(902_008)
      val staleSlug  = ClubSlug("tier-c-closed")
      val freshSlug  = ClubSlug("tier-c-closed-fresh")
      val closedPid  = PlayerId(902_501)
      val livePid    = PlayerId(902_502)
      val closedName = "closed-admin"
      val liveName   = "live-admin-c"
      val responses = Map(
        s"player/$liveName/clubs"  -> apiPlayerClubsJson(List(freshSlug.value)),
        s"club/${freshSlug.value}" -> apiClubJson(ClubId.unwrap(clubId), freshSlug.value)
      )
      for {
        _              <- Club.upsert(Club(clubId, t0, staleSlug, "Closed Admin Test", None, None, None))
        _              <- insertClosedPlayer(closedPid, closedName)
        _              <- insertPlayer(livePid, liveName)
        _              <- insertAdmin(clubId, closedPid)
        _              <- insertAdmin(clubId, livePid)
        client         <- fakeChessComClient(responses)
        result         <- ClubSlugRenameResolver.resolveAndPersist(client, staleSlug, Some(clubId))
        closedFailures <- fetchFailureCountFor(s"player/$closedName")
      } yield assertTrue(
        result.exists((s, _) => s == freshSlug),
        closedFailures == 0L
      )
    }

  private def tierCSkipsSlugsAlreadyKnownToDb =
    test("Tier C: candidate slugs already present in the Club table are skipped (would otherwise verify an unrelated club)") {
      val targetClubId  = ClubId(902_005)
      val knownClubId   = ClubId(902_006)
      val staleSlug     = ClubSlug("tier-c-known-skip")
      val knownSlug     = ClubSlug("admin-other-known-club")
      val freshSlug     = ClubSlug("tier-c-known-skip-fresh")
      val adminPid      = PlayerId(902_301)
      val adminName     = "shared-admin"
      // Admin belongs to BOTH the known-club (already in DB; should be skipped) AND the renamed club (freshSlug).
      // If the resolver fetched the known slug, no `api_fetch_failure` row would still appear (200 OK), but the
      // primary correctness signal is that the result is the rename, not a false None.
      val responses = Map(
        s"player/$adminName/clubs"  -> apiPlayerClubsJson(List(knownSlug.value, freshSlug.value)),
        s"club/${knownSlug.value}"  -> apiClubJson(ClubId.unwrap(knownClubId), knownSlug.value),
        s"club/${freshSlug.value}"  -> apiClubJson(ClubId.unwrap(targetClubId), freshSlug.value)
      )
      for {
        _      <- Club.upsert(Club(targetClubId, t0, staleSlug, "Stale", None, None, None))
        _      <- Club.upsert(Club(knownClubId, t0, knownSlug, "Known", None, None, None))
        _      <- insertPlayer(adminPid, adminName)
        _      <- insertAdmin(targetClubId, adminPid)
        client <- fakeChessComClient(responses)
        result <- ClubSlugRenameResolver.resolveAndPersist(client, staleSlug, Some(targetClubId))
      } yield assertTrue(result.exists((s, _) => s == freshSlug))
    }

  private def tierCAdvancesPastAdminWhose404s =
    test("Tier C: first admin's /clubs 404s → moves to next admin and resolves") {
      val clubId      = ClubId(902_007)
      val staleSlug   = ClubSlug("tier-c-skip-404")
      val freshSlug   = ClubSlug("tier-c-skip-404-fresh")
      val gonePid     = PlayerId(902_401)
      val livePid     = PlayerId(902_402)
      val goneName    = "gone-admin"
      val liveName    = "still-here-admin"
      // gone-admin gets no /clubs route → 404. live-admin returns the rename.
      val responses = Map(
        s"player/$liveName/clubs"  -> apiPlayerClubsJson(List(freshSlug.value)),
        s"club/${freshSlug.value}" -> apiClubJson(ClubId.unwrap(clubId), freshSlug.value)
      )
      for {
        _      <- Club.upsert(Club(clubId, t0, staleSlug, "Stale", None, None, None))
        _      <- insertPlayer(gonePid, goneName)
        _      <- insertPlayer(livePid, liveName)
        _      <- insertAdmin(clubId, gonePid)
        _      <- insertAdmin(clubId, livePid)
        // failures gates `/pub/player/$user/*` to 404 unconditionally.
        client <- fakeChessComClient(responses, failures = Set(goneName))
        result <- ClubSlugRenameResolver.resolveAndPersist(client, staleSlug, Some(clubId))
      } yield assertTrue(result.exists((s, _) => s == freshSlug))
    }

  private def tierCNoOpWithoutHint =
    test("Tier C: no clubIdHint AND stale slug not in Club table → no admin fan-out, returns None") {
      // deriveHint via Club.selectBySlug fails (no row) → effectiveHint = None → Tier C bails immediately.
      val staleSlug = ClubSlug("orphan-stale")
      for {
        client <- fakeChessComClient(Map.empty)
        result <- ClubSlugRenameResolver.resolveAndPersist(client, staleSlug, None)
      } yield assertTrue(result.isEmpty)
    }
}
