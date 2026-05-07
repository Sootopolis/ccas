package ccas.analysis.apps

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, fakeChessComClient}
import ccas.analysis.tables.{Club, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

/** Focused tests for `ClubSlugRenameResolver.resolveOrFetch`. Slug-rename recovery is intentionally not wired
  * through this entry point — see scaladoc on the helper.
  */
object TestClubSlugRenameResolver extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = (suite("TestClubSlugRenameResolver.resolveOrFetch")(
    happyPathLocalHit,
    coldDiscoveryFetchesAndPersists,
    fourOhFourReturnsNone
  ) @@ TestAspect.before(resetTables)).provideShared(
    FreshSchemaLayer("test_club_slug_rename_resolver", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private val resetTables: zio.RIO[PostgresClient, Unit] =
    connectZIO(sql"DELETE FROM club".update.run()).unit

  private val t0 = Instant.parse("2025-01-01T00:00:00Z")

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
}
