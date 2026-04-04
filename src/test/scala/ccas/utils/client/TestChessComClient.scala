package ccas.utils.client

import ccas.utils.sql.PostgresClient
import io.netty.handler.codec.PrematureChannelClosureException
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

import ccas.analysis.tables.{ClientStats, Tables}
import ccas.utils.TestCcasLogger
import ccas.utils.sql.FreshSchemaLayer

import java.time.Instant

object TestChessComClient extends ZIOSpecDefault {

  private case class Payload(value: String)
  private given JsonDecoder[Payload] = DeriveJsonDecoder.gen[Payload]

  private def makeClient(
    handler: Request => Task[Response],
    permits: Long = 5,
    cooldown: Duration = 50.millis,
    retryBase: Duration = 10.millis,
    failureWindowSize: Int = 20,
    failureThreshold: Double = 0.2
  ): ZIO[Scope & PostgresClient, Nothing, (ChessComClient, Ref[ChessComClient.ThrottleState])] =
    for {
      testScope     <- ZIO.service[Scope]
      pgClient      <- ZIO.service[PostgresClient]
      stateRef      <- Ref.make(ChessComClient.ThrottleState(permits, 0, Vector.empty))
      activeRef     <- Ref.make(0)
      rateLimitGate <- Semaphore.make(1)
      lastReqRef    <- Ref.make(0L)
      ema           <- Ref.make(0.0)
      bar           <- TestCcasLogger.noopBar
      stats         <- Ref.make(ChessComClient.StatsAccumulator())
      config = ChessComClient.ThrottleConfig(
        permits,
        cooldown,
        cooldown,
        retryBase,
        10.millis,
        10.millis,
        failureWindowSize,
        failureThreshold,
        10
      )
      refs = ChessComClient.ThrottleRefs(
        stateRef,
        activeRef,
        rateLimitGate,
        lastReqRef,
        ema
      )
    } yield {
      val driver = new ZClient.Driver[Any, Scope, Throwable] {
        override def request(
          version: Version,
          method: Method,
          url: URL,
          headers: Headers,
          body: Body,
          sslConfig: Option[ClientSSLConfig],
          proxy: Option[Proxy]
        )(implicit trace: Trace): ZIO[Scope, Throwable, Response] =
          handler(Request(method = method, url = url, headers = headers, body = body))

        override def socket[Env1 <: Any](
          version: Version,
          url: URL,
          headers: Headers,
          app: WebSocketApp[Env1]
        )(implicit
          trace: Trace,
          ev: Scope =:= Scope
        ): ZIO[Env1 & Scope, Throwable, Response] =
          ZIO.die(new UnsupportedOperationException)
      }
      val client = ChessComClient(
        ZClient.fromDriver(driver),
        pgClient,
        Headers.empty,
        TestCcasLogger.noop,
        refs,
        stats,
        bar,
        config,
        testScope
      )
      (client, stateRef)
    }

  /** Dummy ChessComClient layer that returns 404 for all requests. Useful for tests that need a ChessComClient in the
    * environment but never actually make HTTP calls.
    */
  val dummyLayer: URLayer[PostgresClient, ChessComClient] =
    ZLayer.fromZIO {
      makeClient(_ => ZIO.succeed(Response(status = Status.NotFound))).provideSomeLayer(Scope.default).map(_._1)
    }

  private val testUrl  = URL.decode("http://test.example.com/api").toOption.get
  private val jsonBody = """{"value":"ok"}"""
  private val cfBody =
    """<html><head><title>Just a moment...</title></head><body><script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script></body></html>"""

  override def spec: Spec[TestEnvironment, Any] = suite("TestChessComClient")(
    test("normal 200 succeeds without throttle activation") {
      ZIO.scoped {
        for {
          (client, _) <- makeClient(_ => ZIO.succeed(Response.json(jsonBody)))
          result      <- client.get[Payload](testUrl)
        } yield assertTrue(result.value == "ok")
      }
    },
    test("429 triggers retry and succeeds on subsequent attempt") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).map { n =>
              if (n == 0) { Response(status = Status.TooManyRequests) }
              else { Response.json(jsonBody) }
            }
          }
          result <- client.get[Payload](testUrl)
          count  <- counter.get
        } yield assertTrue(result.value == "ok", count == 2)
      }
    },
    test("exhausted retries surface HttpStatusException") {
      ZIO.scoped {
        for {
          (client, _) <- makeClient(_ => ZIO.succeed(Response(status = Status.TooManyRequests)))
          exit        <- client.get[Payload](testUrl).exit
        } yield assertTrue(exit.isFailure)
      }
    },
    test("failure rate below threshold does not trigger throttle-down") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          // 1 failure out of 15 requests = 6.7% < 20% threshold
          (client, stateRef) <- makeClient(
            handler = { _ =>
              counter.getAndUpdate(_ + 1).map { n =>
                if (n == 5) { Response(status = Status.TooManyRequests) }
                else { Response.json(jsonBody) }
              }
            },
            failureThreshold = 0.2
          )
          urls = (1 to 15).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          _     <- client.getAll[Payload](urls)
          state <- stateRef.get
        } yield assertTrue(state.currentMax == 5L) // unchanged from initial permits
      }
    },
    test("failure rate above threshold triggers throttle-down") {
      ZIO.scoped {
        for {
          // All requests return 429 — coolingDown limits to one drop per cooldown cycle
          (client, stateRef) <- makeClient(
            handler = _ => ZIO.succeed(Response(status = Status.TooManyRequests)),
            permits = 20,
            cooldown = 60.seconds,
            failureThreshold = 0.2
          )
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          state <- stateRef.get
        } yield assertTrue(state.currentMax == 1L, state.coolingDown)
      }
    },
    test("recovery doubles permits after cooldown when failure rate drops") {
      ZIO.scoped {
        for {
          shouldFail <- Ref.make(true)
          (client, stateRef) <- makeClient(
            handler = { _ =>
              shouldFail.get.map { fail =>
                if (fail) { Response(status = Status.TooManyRequests) }
                else { Response.json(jsonBody) }
              }
            },
            permits = 20,
            cooldown = 100.millis,
            failureThreshold = 0.2
          )
          // Phase 1: Sustained failures → throttle down
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          throttledState <- stateRef.get
          throttledMax = throttledState.currentMax
          // Phase 2: Switch to success, fill outcome window
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(6 to 30)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Phase 3: Wait for recovery fibers to fire
          _              <- ZIO.sleep(1.second)
          recoveredState <- stateRef.get
        } yield assertTrue(
          throttledMax < 20L,
          recoveredState.currentMax > throttledMax
        )
      }
    },
    test("generation counter prevents stale recovery") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, stateRef) <- makeClient(
            handler = { _ =>
              counter.getAndUpdate(_ + 1).map { n =>
                // Sustained failures to trigger throttle-down
                if (n < 30) { Response(status = Status.TooManyRequests) }
                else { Response.json(jsonBody) }
              }
            },
            permits = 20,
            cooldown = 500.millis,
            failureThreshold = 0.2
          )
          urls = (1 to 5).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          _     <- client.getAll[Payload](urls).exit // may exhaust retries
          state <- stateRef.get
        } yield assertTrue(
          state.generation >= 1L,
          state.currentMax < 20L
        )
      }
    },
    test("Cloudflare 403 triggers hard throttle-down") {
      ZIO.scoped {
        for {
          (client, stateRef) <- makeClient(
            handler = _ => ZIO.succeed(Response(status = Status.Forbidden, body = Body.fromString(cfBody))),
            permits = 16,
            cooldown = 60.seconds,
            failureThreshold = 0.2
          )
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          state <- stateRef.get
        } yield assertTrue(state.currentMax == 1L, state.coolingDown)
      }
    },
    test("Cloudflare 403 is not retried by single-retry schedule") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).as(Response(status = Status.Forbidden, body = Body.fromString(cfBody)))
          }
          _     <- client.get[Payload](testUrl).exit
          count <- counter.get
          // 1 initial + 2 CF retries = 3 (no extra single-retry on top)
        } yield assertTrue(count == 3)
      }
    },
    test("normal 403 still gets single retry") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).as(Response(status = Status.Forbidden, body = Body.fromString("Forbidden")))
          }
          _     <- client.get[Payload](testUrl).exit
          count <- counter.get
        } yield assertTrue(count == 2)
      }
    },
    test("IOException triggers connection retry and succeeds") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).flatMap { n =>
              if (n == 0) ZIO.fail(java.io.IOException("Connection reset"))
              else ZIO.succeed(Response.json(jsonBody))
            }
          }
          result <- client.get[Payload](testUrl)
          count  <- counter.get
        } yield assertTrue(result.value == "ok", count == 2)
      }
    },
    test("PrematureChannelClosureException triggers connection retry and succeeds") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).flatMap { n =>
              if (n == 0) ZIO.fail(PrematureChannelClosureException("Channel closed"))
              else ZIO.succeed(Response.json(jsonBody))
            }
          }
          result <- client.get[Payload](testUrl)
          count  <- counter.get
        } yield assertTrue(result.value == "ok", count == 2)
      }
    },
    test("non-transient non-HTTP error is not retried") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1) *> ZIO.fail(RuntimeException("unexpected"))
          }
          exit  <- client.get[Payload](testUrl).exit
          count <- counter.get
        } yield assertTrue(exit.isFailure, count == 1)
      }
    },
    test("recovery fibers are interrupted when scope closes") {
      for {
        // Create client and trigger throttle-down inside a scope, then let the scope close
        stateRef <- ZIO.scoped {
          for {
            (client, stateRef) <- makeClient(
              handler = _ => ZIO.succeed(Response(status = Status.TooManyRequests)),
              permits = 20,
              cooldown = 50.millis,
              failureThreshold = 0.2
            )
            _ <- ZIO.foreachParDiscard(1 to 5)(i =>
              client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
            )
            state <- stateRef.get
            _ <- ZIO.succeed(assertTrue(state.currentMax == 1L))
          } yield stateRef // scope closes here, recovery fibers should be interrupted
        }
        // Wait longer than the cooldown — if recovery were alive, it would double permits
        _ <- ZIO.sleep(300.millis)
        state <- stateRef.get
      } yield assertTrue(state.currentMax == 1L)
    },
    test("sequential ordering when throttled to 1 permit") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          order   <- Ref.make(Chunk.empty[Int])
          // currentMax = 1 enforces sequential execution via the gate
          (client, _) <- makeClient(
            handler = { _ =>
              for {
                n <- counter.getAndUpdate(_ + 1)
                _ <- order.update(_ :+ n)
              } yield Response.json(jsonBody)
            },
            permits = 1
          )
          urls = (1 to 3).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          _        <- client.getAll[Payload](urls)
          recorded <- order.get
        } yield assertTrue(recorded == Chunk(0, 1, 2))
      }
    },
    test("pending requests cancel promptly on interruption") {
      ZIO.scoped {
        for {
          served <- Ref.make(0)
          // Slow handler: each request takes 500ms, only 1 permit so requests queue at the gate
          (client, _) <- makeClient(
            handler = { _ =>
              served.update(_ + 1) *> ZIO.sleep(500.millis).as(Response.json(jsonBody))
            },
            permits = 1,
            cooldown = 60.seconds
          )
          urls   = (1 to 20).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          fiber <- client.getAll[Payload](urls).fork
          // Let 1-2 requests get through, then interrupt
          _     <- ZIO.sleep(800.millis)
          _     <- fiber.interrupt
          count <- served.get
          // If gate wait were uninterruptible, all 20 would drain through one-by-one.
          // With interruptible gate, only the ones already admitted should complete.
        } yield assertTrue(count <= 3)
      }
    },
    suiteStatsAccumulator,
    suitePersistStats
  ).provideShared(
    FreshSchemaLayer("test_client", Tables.ensureTables)
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.seconds)

  // ==========================================================================
  // StatsAccumulator (pure)
  // ==========================================================================

  private def suiteStatsAccumulator = suite("StatsAccumulator")(
    test("incError routes 429 to errors429") {
      val s = ChessComClient.StatsAccumulator().incError(429)
      assertTrue(s.errors429 == 1L, s.errors403 == 0L, s.errors404 == 0L)
    },
    test("incError routes 403 to errors403") {
      val s = ChessComClient.StatsAccumulator().incError(403)
      assertTrue(s.errors403 == 1L, s.errors429 == 0L, s.errors404 == 0L)
    },
    test("incError routes 404 to errors404") {
      val s = ChessComClient.StatsAccumulator().incError(404)
      assertTrue(s.errors404 == 1L, s.errors429 == 0L, s.errors403 == 0L)
    },
    test("incError ignores other status codes") {
      val s = ChessComClient.StatsAccumulator().incError(500)
      assertTrue(s.errors429 == 0L, s.errors403 == 0L, s.errors404 == 0L)
    },
    test("recordLatency tracks min and max") {
      val s = ChessComClient.StatsAccumulator()
        .recordLatency(100)
        .recordLatency(50)
        .recordLatency(200)
      assertTrue(
        s.latencyMinMs == 50L,
        s.latencyMaxMs == 200L,
        s.latencySumMs == 350L,
        s.latencyCount == 3L
      )
    },
    test("updatePeak tracks maximum concurrent") {
      val s = ChessComClient.StatsAccumulator()
        .updatePeak(3)
        .updatePeak(1)
        .updatePeak(5)
        .updatePeak(2)
      assertTrue(s.peakConcurrent == 5)
    },
    test("addGateWait accumulates total") {
      val s = ChessComClient.StatsAccumulator().addGateWait(100).addGateWait(50)
      assertTrue(s.gateWaitMs == 150L)
    },
    test("addEmaDelay accumulates total") {
      val s = ChessComClient.StatsAccumulator().addEmaDelay(200).addEmaDelay(30)
      assertTrue(s.emaDelayMs == 230L)
    },
    test("addThrottled accumulates total") {
      val s = ChessComClient.StatsAccumulator().addThrottled(5000).addThrottled(3000)
      assertTrue(s.throttledMs == 8000L)
    }
  )

  // ==========================================================================
  // persistStats (DB)
  // ==========================================================================

  private def makeFlushContext(
    appLabel: String,
    statsRef: Ref[ChessComClient.StatsAccumulator],
    pgClient: PostgresClient
  ): ZIO[Any, Nothing, ChessComClient.FlushContext] =
    for {
      rowIdRef    <- Ref.make(Option.empty[Long])
      configIdRef <- Ref.make(Option.empty[Long])
      stateRef    <- Ref.make(ChessComClient.ThrottleState(8, 0, Vector.empty))
      startedAt   <- ZIO.succeed(Instant.now())
      config = ChessComClient.ThrottleConfig(8, 30.seconds, 5.seconds, 1.second, 5.seconds, 10.seconds, 20, 0.2, 10)
    } yield ChessComClient.FlushContext(appLabel, startedAt, statsRef, rowIdRef, configIdRef, config, stateRef, pgClient)

  private def suitePersistStats = suite("persistStats")(
    test("inserts on first call and updates on subsequent calls") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ChessComClient.StatsAccumulator().copy(requests = 5, successes = 4, failures = 1))
        ctx      <- makeFlushContext("test-persist", statsRef, pgClient)
        // First flush: should insert config + stats
        _         <- ChessComClient.persistStats(ctx)
        rowId1    <- ctx.statsRowId.get
        configId1 <- ctx.configIdRef.get
        // Second flush after more requests: should update same stats row, reuse config
        _         <- statsRef.update(_.copy(requests = 10, successes = 9))
        _         <- ChessComClient.persistStats(ctx)
        rowId2    <- ctx.statsRowId.get
        configId2 <- ctx.configIdRef.get
        recent    <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
      } yield {
        val row = recent.find(_.appLabel == "test-persist").get
        assertTrue(
          rowId1.isDefined,
          rowId2 == rowId1,
          configId1.isDefined,
          configId2 == configId1,
          recent.count(_.appLabel == "test-persist") == 1,
          row.requests == 10L,
          row.configId == configId1.get
        )
      }
    },
    test("skips persist when no requests made") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ChessComClient.StatsAccumulator())
        ctx      <- makeFlushContext("test-noop", statsRef, pgClient)
        _      <- ChessComClient.persistStats(ctx)
        rowId  <- ctx.statsRowId.get
        recent <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
      } yield assertTrue(
        rowId.isEmpty,
        recent.count(_.appLabel == "test-noop") == 0
      )
    }
  )
}
