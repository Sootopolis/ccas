package ccas.utils.client

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.*
import zio.http.*
import zio.test.*

import ccas.analysis.tables.{ApiFetchFailure, ApiResponseBody, ApiResponseCache, Tables}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO
import ccas.utils.client.TestChessComClientSupport.*

/** #200: an object-store outage must take the *cache* offline, not the app. The body cache is not source of truth —
  * a lost body just re-fetches from Chess.com — so a [[BodyStore]] error has to degrade to a cache miss (read) or a
  * skipped write, never surface as a failed request.
  *
  * These drive the real `ChessComClient` / table code against a [[FaultyBodyStore]] wrapping the suite's working
  * filesystem store, so the whole path is covered: `getCacheableImpl` → `loadById` / `upsertWithBody` → accessor.
  */
object TestBodyStoreOutage extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] = suite("TestBodyStoreOutage")(
    suiteReadOutage,
    suiteWriteOutage,
    suiteStartup
  ).provideShared(
    FreshSchemaLayer("test_body_store_outage", Tables.ensureTables)
  // `sequential` for the same reason as TestChessComClientCaching: one shared schema + one shared fs store, so
  // parallel siblings would race each other's cache rows and body objects.
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.seconds)

  private def cacheable200(body: String, maxAge: Int): Response =
    Response(
      status = Status.Ok,
      headers = Headers(
        Header.ContentType(MediaType.application.json),
        Header.CacheControl.MaxAge(maxAge),
        Header.ETag.Strong("outage-v1")
      ),
      body = Body.fromString(body)
    )

  /** A [[ChessComClient]] wired to a [[FaultyBodyStore]] instead of the suite's store, so the client's own cache
    * reads and writes can be broken mid-test. Starts healthy — most tests populate real data first.
    */
  private def faultyStoreClient(
    handler: Request => Task[Response]
  ): ZIO[
    Scope & PostgresClient & BodyStore,
    Nothing,
    (ChessComClient, FaultyBodyStore, Ref[ClientStatsAccumulator])
  ] =
    for {
      working <- ZIO.service[BodyStore]
      faulty  <- FaultyBodyStore.make(working)
      // Wrapped in the production decorator, so these drive the same stack `BodyStore.live` builds and a stall is a
      // real deadline breach rather than merely a slow call. The budget is far above what the suite's local
      // filesystem store takes while healthy, so only a deliberate `stallReads`/`stallWrites` can trip it.
      bounded = new BodyStore.Deadlines(faulty, BodyStore.BodyStoreLimits(read = 500.millis, write = 500.millis))
      built <- makeClient(handler).provideSomeEnvironment[Scope & PostgresClient](_.add[BodyStore](bounded))
    } yield (built._1, faulty, built._3)

  private def bodyRowCount: ZIO[PostgresClient, Throwable, Long] =
    connectZIO(sql"SELECT count(*) FROM api_response_body".query[Long].run().head)

  // Addressed by hash rather than by row count: the startup test must not depend on which other rows happen to
  // exist, nor on sibling tests leaving the Cloudflare body unreferenced.
  private def cfPointerExists: ZIO[PostgresClient, Throwable, Boolean] =
    connectZIO {
      val hash = ApiResponseBody.CfCanonicalHash
      sql"SELECT 1 FROM api_response_body WHERE body_hash = $hash".query[Int].run().nonEmpty
    }

  private def deleteCfPointer: ZIO[PostgresClient, Throwable, Int] =
    connectZIO {
      val hash = ApiResponseBody.CfCanonicalHash
      sql"DELETE FROM api_response_body WHERE body_hash = $hash".update.run()
    }

  private def suiteReadOutage = suite("read outage")(
    test("a Fresh hit whose body can't be read refetches from the network instead of failing") {
      val url  = URL.decode("http://test.example.com/api/outage/read-refetch").toOption.get
      val body = """{"value":"read-outage"}"""
      ZIO.scoped {
        for {
          netCalls              <- Ref.make(0)
          (client, faulty, sts) <- faultyStoreClient(_ => netCalls.update(_ + 1).as(cacheable200(body, maxAge = 3600)))
          _                     <- client.getCacheable[Payload](url) // populate while healthy (network #1)
          _                     <- faulty.breakReads
          fresh                 <- client.getCacheable[Payload](url) // metadata-only hit; body not loaded yet
          value                 <- fresh.getValue                    // body unreadable → refetch (network #2)
          calls                 <- netCalls.get
          meta                  <- ApiResponseCache.lookupMeta(url.encode)
          stats                 <- sts.get
        } yield assertTrue(
          fresh.isInstanceOf[CacheableResult.Fresh[?]],
          value.value == "read-outage",
          calls == 2,
          meta.isDefined,
          // The hit was counted on the metadata lookup, before the body was read, so it has to be reconciled:
          // `cacheUnserved` is what keeps `cache_hits` from claiming an entry that in fact cost a full round trip.
          stats.cacheHits == 1,
          stats.cacheUnserved == 1,
          stats.requests == 2
        )
      }
    },
    test("an unreadable body keeps its cache entry, so the outage doesn't outlive itself") {
      // #215. Worst case: the store is down for the whole request, so the refetch can't repopulate either. The
      // caller must still get its value — every Chess.com byte is in memory by then — and, crucially, the metadata
      // row must SURVIVE. It is still accurate; only the body is unreachable. Dropping it would throw away the ETag
      // and max-age, so every URL touched during an outage would pay a full unconditional GET after recovery
      // instead of a cheap 304 — the outage's cost billed to the Chess.com rate limit long after it ended.
      val url  = URL.decode("http://test.example.com/api/outage/read-and-write-down").toOption.get
      val body = """{"value":"fully-down"}"""
      ZIO.scoped {
        for {
          netCalls              <- Ref.make(0)
          (client, faulty, sts) <- faultyStoreClient(_ => netCalls.update(_ + 1).as(cacheable200(body, maxAge = 3600)))
          _                     <- client.getCacheable[Payload](url) // populate while healthy
          before                <- ApiResponseCache.lookupMeta(url.encode)
          _                     <- faulty.breakReads
          _                     <- faulty.breakWrites
          value                 <- client.get[Payload](url)
          calls                 <- netCalls.get
          after                 <- ApiResponseCache.lookupMeta(url.encode)
          stats                 <- sts.get
        } yield assertTrue(
          value.value == "fully-down",
          calls == 2,
          after.isDefined,
          // The validators specifically — they are the whole reason keeping the row is worth anything.
          after.flatMap(_.etag) == before.flatMap(_.etag),
          after.flatMap(_.maxAgeSeconds) == before.flatMap(_.maxAgeSeconds),
          stats.cacheUnserved == 1
        )
      }
    },
    test("a store too slow to answer takes the same path as a broken one") {
      // #211 meets #215: a deadline breach is a third way for a read to come back empty, and it is an *unavailable*,
      // not a *missing*. If it were treated as missing, a slow store — which times out far more often than a broken
      // one errors — would delete metadata faster than an outage does.
      val url  = URL.decode("http://test.example.com/api/outage/read-stalled").toOption.get
      val body = """{"value":"stalled"}"""
      ZIO.scoped {
        for {
          netCalls              <- Ref.make(0)
          (client, faulty, sts) <- faultyStoreClient(_ => netCalls.update(_ + 1).as(cacheable200(body, maxAge = 3600)))
          _                     <- client.getCacheable[Payload](url) // populate while responsive
          _                     <- faulty.stallReads(2.seconds)
          fresh                 <- client.getCacheable[Payload](url)
          value                 <- fresh.getValue
          _                     <- faulty.stallReads(Duration.Zero)
          calls                 <- netCalls.get
          meta                  <- ApiResponseCache.lookupMeta(url.encode)
          stats                 <- sts.get
        } yield assertTrue(
          value.value == "stalled",
          calls == 2,
          meta.isDefined,
          stats.cacheUnserved == 1
        )
      }
    },
    test("selectRecent yields a body-less failure row when the store is unreadable") {
      val url = "http://test.example.com/api/outage/failure-body-unreadable"
      for {
        pgClient <- ZIO.service[PostgresClient]
        working  <- ZIO.service[BodyStore]
        faulty   <- FaultyBodyStore.make(working)
        now      <- Clock.instant
        failure   = ApiFetchFailure(now, url, "HttpStatusException", Some("404"), Some("""{"code":0}"""))
        _        <- ApiFetchFailure.insert(failure).provideEnvironment(ZEnvironment(pgClient, faulty))
        _        <- faulty.breakReads
        rows     <- ApiFetchFailure.selectRecent(now).provideEnvironment(ZEnvironment(pgClient, faulty))
      } yield assertTrue(
        rows.exists(r => r.url == url && r.responseBody.isEmpty)
      )
    }
  )

  private def suiteWriteOutage = suite("write outage")(
    test("a 200 whose body can't be stored returns Changed and persists nothing") {
      val url  = URL.decode("http://test.example.com/api/outage/write-skipped").toOption.get
      val body = """{"value":"write-outage"}"""
      ZIO.scoped {
        for {
          netCalls              <- Ref.make(0)
          (client, faulty, sts) <- faultyStoreClient(_ => netCalls.update(_ + 1).as(cacheable200(body, maxAge = 3600)))
          _                     <- faulty.breakWrites
          bodiesBefore          <- bodyRowCount
          result                <- client.getCacheable[Payload](url)
          value                 <- result.getValue
          meta                  <- ApiResponseCache.lookupMeta(url.encode)
          _                     <- client.getCacheable[Payload](url) // nothing cached, so it must hit the network again
          calls                 <- netCalls.get
          bodiesAfter           <- bodyRowCount
          stats                 <- sts.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Changed[?]],
          value.value == "write-outage",
          meta.isEmpty,
          calls == 2,
          // No dangling hash-pointer row either: a pointer with no object behind it is a cache entry that can only
          // ever produce a refetch, so `upsertWithBody` skips the whole write rather than half of it.
          bodiesAfter == bodiesBefore,
          // A write outage claims nothing was served, so it must not touch `cacheUnserved` — that counter exists to
          // retract an optimistic *hit*, and there was never one here.
          stats.cacheUnserved == 0,
          stats.cacheHits == 0
        )
      }
    },
    test("the cache repopulates on the first successful write after the outage") {
      val url  = URL.decode("http://test.example.com/api/outage/write-self-heal").toOption.get
      val body = """{"value":"self-heal"}"""
      ZIO.scoped {
        for {
          netCalls              <- Ref.make(0)
          (client, faulty, sts) <- faultyStoreClient(_ => netCalls.update(_ + 1).as(cacheable200(body, maxAge = 3600)))
          _                     <- faulty.breakWrites
          _                     <- client.getCacheable[Payload](url) // network #1, uncached
          during                <- ApiResponseCache.lookupMeta(url.encode)
          _                     <- faulty.healWrites
          _                     <- client.getCacheable[Payload](url) // network #2, cached this time
          after                 <- ApiResponseCache.lookupMeta(url.encode)
          fresh                 <- client.getCacheable[Payload](url) // served from cache, no network
          value                 <- fresh.getValue
          calls                 <- netCalls.get
          stats                 <- sts.get
        } yield assertTrue(
          during.isEmpty,
          after.isDefined,
          fresh.isInstanceOf[CacheableResult.Fresh[?]],
          value.value == "self-heal",
          calls == 2,
          // A genuine hit, served end to end: nothing to reconcile.
          stats.cacheHits == 1,
          stats.cacheUnserved == 0
        )
      }
    },
    test("a fetch failure is still recorded when its body can't be stored") {
      val url = "http://test.example.com/api/outage/failure-body-unstorable"
      for {
        pgClient <- ZIO.service[PostgresClient]
        working  <- ZIO.service[BodyStore]
        faulty   <- FaultyBodyStore.make(working)
        now      <- Clock.instant
        _        <- faulty.breakWrites
        failure   = ApiFetchFailure(now, url, "HttpStatusException", Some("429"), Some("rate limited"))
        inserted <- ApiFetchFailure.insert(failure).provideEnvironment(ZEnvironment(pgClient, faulty))
        _        <- faulty.healWrites
        rows     <- ApiFetchFailure.selectRecent(now).provideEnvironment(ZEnvironment(pgClient, faulty))
      } yield assertTrue(
        inserted == 1,
        // The audit trail is the point of the table; the body is a bonus we drop rather than lose the row over.
        rows.exists(r => r.url == url && r.errorMessage.contains("429") && r.responseBody.isEmpty)
      )
    }
  )

  private def suiteStartup = suite("startup")(
    test("normalizeCfBodies tolerates an unreachable store instead of failing boot") {
      // `ensureTables` already pre-warmed the Cloudflare canonical body, so drop that exact pointer row first and
      // assert on its presence by hash — no row-count arithmetic, and no dependence on sibling tests or on the
      // order this suite's suites run in. (A future sibling that referenced the CF body would fail the DELETE on
      // the FK, loudly, rather than silently invert the assertions.)
      for {
        pgClient  <- ZIO.service[PostgresClient]
        working   <- ZIO.service[BodyStore]
        faulty    <- FaultyBodyStore.make(working)
        env        = ZEnvironment(pgClient, faulty)
        _         <- deleteCfPointer
        _         <- faulty.breakWrites
        skipped   <- ApiResponseBody.normalizeCfBodies.provideEnvironment(env)
        afterSkip <- cfPointerExists
        _         <- faulty.healWrites
        restored  <- ApiResponseBody.normalizeCfBodies.provideEnvironment(env)
        afterHeal <- cfPointerExists
      } yield assertTrue(
        skipped == 0,
        !afterSkip, // the outage inserted no pointer row for an object that was never stored
        restored == 1,
        afterHeal
      )
    }
  )
}
