package ccas.analysis.tables

import java.sql.SQLException
import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.ZIO
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.subtypes.ApiResponseBodyId
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

object TestApiResponseCacheSql extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestApiResponseCacheSql")(
    testUpsertInserts,
    testLookupMetaReturnsRow,
    testUpsertUpdatesExistingRow,
    testUpsertDedupedSameBodyReturnsSameBodyId,
    testTouchUpdatesFetchedAtOnly,
    testInvalidateDeletes,
    testFkRestrictBlocksBodyDelete,
    testOrphanCleanupPreservesCacheReferencedBody,
    testNoStoreHeaderUpsertStillWorksWhenCallerPassesItExplicitly
  ).provideShared(
    FreshSchemaLayer("test_api_response_cache", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofMinutes(5))
    val t2: Instant = t0.plus(Duration.ofHours(1))
  }

  private val urlA = "https://api.chess.com/pub/club/devon-chess"
  private val urlB = "https://api.chess.com/pub/club/devon-chess/members"
  private val bodyA = """{"@id":"...","name":"Devon Chess"}"""
  private val bodyB = """{"weekly":[],"monthly":[],"all_time":[]}"""

  private def countCache: ZIO[PostgresClient, SQLException, Int] =
    connectZIO(sql"SELECT COUNT(*) FROM api_response_cache".query[Long].run().head.toInt)

  private def countBodies: ZIO[PostgresClient, SQLException, Int] =
    connectZIO(sql"SELECT COUNT(*) FROM api_response_body".query[Long].run().head.toInt)

  private def wipeCache: ZIO[PostgresClient, SQLException, Unit] =
    for {
      _ <- connectZIO(sql"DELETE FROM api_fetch_failure".update.run())
      _ <- connectZIO(sql"DELETE FROM api_response_cache".update.run())
      _ <- connectZIO(sql"DELETE FROM api_response_body".update.run())
    } yield ()

  private def testUpsertInserts = test("upsertWithBody inserts a new row and dedupes body storage") {
    for {
      _        <- wipeCache
      bodyId   <- ApiResponseCache.upsertWithBody(
        url = urlA,
        body = bodyA,
        etag = Some("\"abc\""),
        lastModified = Some(Times.t0),
        maxAgeSeconds = Some(3600L),
        contentType = Some("application/json"),
        fetchedAt = Times.t0
      )
      rawId    = ApiResponseBodyId.unwrap(bodyId)
      cache    <- countCache
      bodies   <- countBodies
    } yield assertTrue(
      cache == 1,
      bodies == 1,
      rawId > 0L
    )
  }

  private def testLookupMetaReturnsRow = test("lookupMeta returns the upserted metadata without joining the body") {
    for {
      metaOpt <- ApiResponseCache.lookupMeta(urlA)
    } yield assertTrue(
      metaOpt.isDefined,
      metaOpt.exists(_.url == urlA),
      metaOpt.exists(_.etag.contains("\"abc\"")),
      metaOpt.exists(_.maxAgeSeconds.contains(3600L)),
      metaOpt.exists(_.contentType.contains("application/json"))
    )
  }

  private def testUpsertUpdatesExistingRow = test("upsertWithBody on the same URL updates validators and body_id") {
    for {
      newBodyId <- ApiResponseCache.upsertWithBody(
        url = urlA,
        body = bodyB,
        etag = Some("\"def\""),
        lastModified = Some(Times.t1),
        maxAgeSeconds = Some(7200L),
        contentType = Some("application/json"),
        fetchedAt = Times.t1
      )
      cache  <- countCache
      bodies <- countBodies
      meta   <- ApiResponseCache.lookupMeta(urlA)
    } yield assertTrue(
      cache == 1, // still one row, was updated in place
      bodies == 2, // original body and new body both present
      meta.exists(_.etag.contains("\"def\"")),
      meta.exists(_.maxAgeSeconds.contains(7200L)),
      meta.exists(_.bodyId == newBodyId)
    )
  }

  private def testUpsertDedupedSameBodyReturnsSameBodyId =
    test("upsertWithBody with a byte-identical body re-uses the existing api_response_body row") {
      for {
        initial <- ApiResponseCache.upsertWithBody(
          url = urlB,
          body = bodyA, // same content as urlA's original
          etag = Some("\"xyz\""),
          lastModified = Some(Times.t0),
          maxAgeSeconds = Some(3600L),
          contentType = Some("application/json"),
          fetchedAt = Times.t0
        )
        again <- ApiResponseCache.upsertWithBody(
          url = urlB,
          body = bodyA,
          etag = Some("\"xyz\""),
          lastModified = Some(Times.t0),
          maxAgeSeconds = Some(3600L),
          contentType = Some("application/json"),
          fetchedAt = Times.t2
        )
        cache  <- countCache
        bodies <- countBodies
      } yield assertTrue(
        initial == again,
        cache == 2, // urlA + urlB
        bodies == 2 // bodyA (shared) + bodyB (from urlA's second upsert)
      )
    }

  private def testTouchUpdatesFetchedAtOnly = test("touch refreshes fetched_at but leaves validators alone") {
    for {
      before  <- ApiResponseCache.lookupMeta(urlA)
      touched <- ApiResponseCache.touch(urlA, Times.t2)
      after   <- ApiResponseCache.lookupMeta(urlA)
    } yield assertTrue(
      touched == 1,
      before.exists(_.fetchedAt == Times.t1),
      after.exists(_.fetchedAt == Times.t2),
      before.flatMap(_.etag) == after.flatMap(_.etag),
      before.map(_.bodyId) == after.map(_.bodyId)
    )
  }

  private def testInvalidateDeletes = test("invalidate removes the cache row") {
    for {
      deleted <- ApiResponseCache.invalidate(urlA)
      after   <- ApiResponseCache.lookupMeta(urlA)
    } yield assertTrue(
      deleted == 1,
      after.isEmpty
    )
  }

  private def testFkRestrictBlocksBodyDelete =
    test("DELETE FROM api_response_body violates FK while cache still references the row") {
      // Reset so the ordering is deterministic: one cache row pointing at one body row.
      for {
        _ <- wipeCache
        _ <- ApiResponseCache.upsertWithBody(
          url = urlA,
          body = bodyA,
          etag = None,
          lastModified = None,
          maxAgeSeconds = Some(300L),
          contentType = Some("application/json"),
          fetchedAt = Times.t0
        )
        failed <- connectZIO(sql"DELETE FROM api_response_body".update.run()).exit
      } yield assertTrue(failed.isFailure)
    }

  private def testOrphanCleanupPreservesCacheReferencedBody =
    test("ApiResponseBody.deleteOrphans preserves bodies still referenced by the cache") {
      for {
        deleted <- ApiResponseBody.deleteOrphans
        cache   <- countCache
        bodies  <- countBodies
      } yield assertTrue(
        deleted == 0, // nothing deleted — body is cache-referenced
        cache == 1,
        bodies == 1
      )
    }

  private def testNoStoreHeaderUpsertStillWorksWhenCallerPassesItExplicitly =
    test("upsertWithBody accepts None validators / max-age for responses without cache headers") {
      for {
        _ <- wipeCache
        bodyId <- ApiResponseCache.upsertWithBody(
          url = urlB,
          body = bodyB,
          etag = None,
          lastModified = None,
          maxAgeSeconds = None,
          contentType = None,
          fetchedAt = Times.t0
        )
        meta <- ApiResponseCache.lookupMeta(urlB)
      } yield assertTrue(
        ApiResponseBodyId.unwrap(bodyId) > 0L,
        meta.exists(_.etag.isEmpty),
        meta.exists(_.maxAgeSeconds.isEmpty),
        meta.exists(_.contentType.isEmpty),
        meta.exists(_.bodyId == bodyId)
      )
    }
}
