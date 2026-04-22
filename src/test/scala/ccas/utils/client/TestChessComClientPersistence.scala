package ccas.utils.client

import java.time.Instant

import ccas.utils.sql.PostgresClient
import zio.*
import zio.test.*

import ccas.analysis.tables.{ClientConfig, ClientStats, Tables}
import ccas.utils.sql.FreshSchemaLayer

object TestChessComClientPersistence extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] = suite("TestChessComClientPersistence")(
    suitePersistStats
  ).provideShared(
    FreshSchemaLayer("test_client_persistence", Tables.ensureTables)
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.seconds)

  private def makeFlushContext(
    appLabel: String,
    statsRef: Ref[ClientStatsAccumulator],
    pgClient: PostgresClient
  ): ZIO[Any, Nothing, ClientStatsFlushContext] =
    for {
      configIdRef <- Ref.make(Option.empty[Long])
      stateRef    <- Ref.make(ChessComClient.ThrottleState(8, 0, Vector.empty))
      startedAt   <- ZIO.succeed(Instant.now())
      config = ChessComClient.ThrottleConfig(Vector(2, 4, 8), 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10, 0, Duration.Zero)
    } yield ClientStatsFlushContext(
      s"test-$appLabel", appLabel, startedAt, statsRef, configIdRef, config, stateRef, pgClient
    )

  private def suitePersistStats = suite("persistStats")(
    test("repeated flushes upsert a single cumulative row") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(
          ClientStatsAccumulator().copy(
            requests = 5, successes = 4, failures = 1, activeMs = 500,
            attemptsByTier = Map(8 -> 6L, 4 -> 2L),
            errors429ByTier = Map(8 -> 1L)
          )
        )
        ctx      <- makeFlushContext("test-upsert", statsRef, pgClient)
        _         <- ClientStatsPersistence.persistStats(ctx)
        configId1 <- ctx.configIdRef.get
        // Second flush after more requests: should UPDATE the same row
        _         <- statsRef.update(_.copy(
          requests = 12, successes = 11, activeMs = 1200,
          attemptsByTier = Map(8 -> 10L, 4 -> 5L),
          errors429ByTier = Map(8 -> 2L, 4 -> 1L)
        ))
        _         <- ClientStatsPersistence.persistStats(ctx)
        configId2 <- ctx.configIdRef.get
        recent    <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
        rows       = recent.filter(_.appLabel == "test-upsert")
      } yield assertTrue(
        configId1.isDefined,
        configId2 == configId1,
        rows.size == 1,
        rows(0).requests == 12L,
        rows(0).successes == 11L,
        rows(0).activeMs == 1200L,
        rows(0).configId == configId1.get,
        rows(0).attemptsByTier == "4:5|8:10",
        rows(0).errors429ByTier == "4:1|8:2"
      )
    },
    test("flush does not mutate the stats accumulator") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(
          ClientStatsAccumulator().copy(requests = 3, successes = 3, activeMs = 300)
            .updatePeak(5).recordLatency(100).recordLatency(200)
        )
        ctx <- makeFlushContext("test-no-mutate", statsRef, pgClient)
        _   <- ClientStatsPersistence.persistStats(ctx)
        s   <- statsRef.get
      } yield assertTrue(
        s.peakConcurrent == 5,
        s.latencyMinMs == 100L,
        s.latencyMaxMs == 200L,
        s.latencyBuckets == Vector(0L, 0L, 1L, 1L, 0L, 0L),
        s.requests == 3L,
        s.successes == 3L
      )
    },
    test("ensureConfig deduplicates identical configs") {
      val cc = {
        val c = ClientConfig(0L, "", List(2, 4, 99), 0, 88, 77, 0, 33, 0.5, 22, 66, 55, 44, 5, 2, 3)
        c.copy(configHash = c.computeHash)
      }
      for {
        id1 <- ClientConfig.ensureConfig(cc)
        id2 <- ClientConfig.ensureConfig(cc)
      } yield assertTrue(id1 == id2)
    },
    test("separate sessions with same config reuse existing config row") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        stats1   <- Ref.make(ClientStatsAccumulator().copy(requests = 3, successes = 3, activeMs = 300))
        ctx1     <- makeFlushContext("test-dedup-1", stats1, pgClient)
        _        <- ClientStatsPersistence.persistStats(ctx1)
        cid1     <- ctx1.configIdRef.get
        stats2   <- Ref.make(ClientStatsAccumulator().copy(requests = 7, successes = 7, activeMs = 700))
        ctx2     <- makeFlushContext("test-dedup-2", stats2, pgClient)
        _        <- ClientStatsPersistence.persistStats(ctx2)
        cid2     <- ctx2.configIdRef.get
      } yield assertTrue(
        cid1.isDefined,
        cid2 == cid1
      )
    },
    test("in-progress throttle is included without mutating accumulator") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ClientStatsAccumulator().copy(requests = 5, successes = 5, activeMs = 500))
        ctx      <- makeFlushContext("test-ongoing-throttle", statsRef, pgClient)
        nowMs <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
        _     <- ctx.stateRef.update(_.copy(
          currentMax = 1, coolingDown = true, throttledSince = Some(nowMs - 100_000)
        ))
        _ <- ClientStatsPersistence.persistStats(ctx)
        recent <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
        row     = recent.filter(_.appLabel == "test-ongoing-throttle").head
        s      <- statsRef.get
      } yield assertTrue(
        // Row includes ~100s of in-progress throttle
        row.throttledMs >= 95_000L && row.throttledMs <= 105_000L,
        // Accumulator itself is unchanged (no in-progress baked in)
        s.throttledMs == 0L
      )
    },
    test("skips persist when no requests made") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ClientStatsAccumulator())
        ctx      <- makeFlushContext("test-noop", statsRef, pgClient)
        _      <- ClientStatsPersistence.persistStats(ctx)
        recent <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
      } yield assertTrue(
        recent.count(_.appLabel == "test-noop") == 0
      )
    },
    test("persists when cache-only activity (requests = 0, cacheHits > 0)") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ClientStatsAccumulator().copy(cacheHits = 3))
        ctx      <- makeFlushContext("test-cache-only", statsRef, pgClient)
        _      <- ClientStatsPersistence.persistStats(ctx)
        recent <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
        row     = recent.find(_.appLabel == "test-cache-only")
      } yield assertTrue(
        row.exists(_.requests == 0L),
        row.exists(_.cacheHits == 3L)
      )
    }
  )
}
