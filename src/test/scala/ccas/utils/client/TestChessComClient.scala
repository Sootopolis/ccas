package ccas.utils.client

import ccas.utils.sql.PostgresClient
import io.netty.handler.codec.PrematureChannelClosureException
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

import ccas.analysis.tables.{ClientConfig, ClientStats, Tables}
import ccas.utils.TestCcasLogger
import ccas.utils.sql.FreshSchemaLayer

import java.time.Instant

object TestChessComClient extends ZIOSpecDefault {

  private case class Payload(value: String)
  private given JsonDecoder[Payload] = DeriveJsonDecoder.gen[Payload]

  /** Generate doubling recovery tiers up to `max`, e.g. max=20 → Vector(2, 4, 8, 16, 20). */
  private def doublingTiers(max: Int): Vector[Int] = {
    val base  = Iterator.iterate(2)(_ * 2).takeWhile(_ < max.max(2)).toVector
    val tiers = base :+ max.max(2)
    tiers.distinct
  }

  private def makeClient(
    handler: Request => Task[Response],
    permits: Long = 5,
    cooldown: Duration = 50.millis,
    retryBase: Duration = 10.millis,
    failureWindowSize: Int = 20,
    failureThreshold: Double = 0.2,
    recoveryTiers: Option[Vector[Int]] = None,
    max429Retries: Int = 5,
    maxCfRetries: Int = 2,
    maxConnectionRetries: Int = 3
  ): ZIO[Scope & PostgresClient, Nothing, (ChessComClient, Ref[ChessComClient.ThrottleState], Ref[ChessComClient.StatsAccumulator])] =
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
        recoveryTiers.getOrElse(doublingTiers(permits.toInt)),
        cooldown,
        cooldown,
        retryBase,
        10.millis,
        retryBase,
        max429Retries,
        maxCfRetries,
        maxConnectionRetries,
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
      (client, stateRef, stats)
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
          (client, _, _) <- makeClient(_ => ZIO.succeed(Response.json(jsonBody)))
          result      <- client.get[Payload](testUrl)
        } yield assertTrue(result.value == "ok")
      }
    },
    test("429 triggers retry and succeeds on subsequent attempt") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _, statsRef) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).map { n =>
              if (n == 0) { Response(status = Status.TooManyRequests) }
              else { Response.json(jsonBody) }
            }
          }
          result <- client.get[Payload](testUrl)
          count  <- counter.get
          s      <- statsRef.get
          // 1 request, 2 attempts (first 429, second succeeds)
        } yield assertTrue(
          result.value == "ok", count == 2,
          s.requests == 1L, s.attempts == 2L, s.errors429 == 1L, s.successes == 1L, s.failures == 0L
        )
      }
    },
    test("attempts and 429s are attributed to the tier active at the time") {
      ZIO.scoped {
        for {
          (client, _, statsRef) <- makeClient(_ => ZIO.succeed(Response(status = Status.TooManyRequests)))
          _ <- client.get[Payload](testUrl).exit
          s <- statsRef.get
          // permits=5 (default), single URL, 1 initial + 5 retries = 6 attempts all at tier 5
        } yield assertTrue(
          s.attempts == 6L,
          s.attemptsByTier == Map(5 -> 6L),
          s.errors429 == 6L,
          s.errors429ByTier == Map(5 -> 6L)
        )
      }
    },
    test("exhausted retries surface HttpStatusException") {
      ZIO.scoped {
        for {
          (client, _, statsRef) <- makeClient(_ => ZIO.succeed(Response(status = Status.TooManyRequests)))
          exit <- client.get[Payload](testUrl).exit
          s    <- statsRef.get
          // 1 request, 1 initial + 5 retries = 6 attempts, all 429
        } yield assertTrue(exit.isFailure, s.requests == 1L, s.attempts == 6L, s.errors429 == 6L, s.failures == 1L)
      }
    },
    test("failure rate below threshold does not trigger throttle-down") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          // 1 failure out of 15 requests = 6.7% < 20% threshold
          (client, stateRef, _) <- makeClient(
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
          (client, stateRef, _) <- makeClient(
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
    test("recovery walks configured tiers, not doubling") {
      ZIO.scoped {
        for {
          shouldFail <- Ref.make(true)
          // Tiers [3, 5] — doubling from 1 would give 2, but tier-based gives 3
          (client, stateRef, _) <- makeClient(
            handler = { _ =>
              shouldFail.get.map { fail =>
                if (fail) Response(status = Status.TooManyRequests)
                else Response.json(jsonBody)
              }
            },
            permits = 5,
            cooldown = 1.second,
            failureThreshold = 0.2,
            recoveryTiers = Some(Vector(3, 5))
          )
          // Phase 1: trigger throttle-down → currentMax = 1
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          throttled <- stateRef.get
          // Phase 2: flush failures from the rolling window with enough successes
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(6 to 35)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Wait 1 cooldown + buffer: exactly one recovery step fires
          _              <- ZIO.sleep(1200.millis)
          afterFirstStep <- stateRef.get
        } yield assertTrue(
          throttled.currentMax == 1L,
          afterFirstStep.currentMax == 3L  // tier value, NOT 2 that doubling would give
        )
      }
    },
    test("recovery doubles permits after cooldown when failure rate drops") {
      ZIO.scoped {
        for {
          shouldFail <- Ref.make(true)
          (client, stateRef, _) <- makeClient(
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
          (client, stateRef, _) <- makeClient(
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
          (client, stateRef, _) <- makeClient(
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
    test("Cloudflare 403 is retried by the CF schedule (2 retries)") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _, statsRef) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).as(Response(status = Status.Forbidden, body = Body.fromString(cfBody)))
          }
          _     <- client.get[Payload](testUrl).exit
          count <- counter.get
          s     <- statsRef.get
          // 1 initial + 2 CF retries = 3 attempts, all CF 403
        } yield assertTrue(
          count == 3, s.requests == 1L, s.attempts == 3L,
          s.errorsCf403 == 3L, s.errors403 == 0L, s.failures == 1L
        )
      }
    },
    test("non-CF 403 is not retried and is not a throttle signal") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, stateRef, statsRef) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).as(Response(status = Status.Forbidden, body = Body.fromString("Forbidden")))
          }
          _     <- client.get[Payload](testUrl).exit
          count <- counter.get
          s     <- statsRef.get
          state <- stateRef.get
          // 1 attempt, no retries; non-CF 403 is treated like any non-rate-limit response:
          // it contributes to the outcome window as a non-failure (like 404 or 500).
        } yield assertTrue(
          count == 1, s.requests == 1L, s.attempts == 1L, s.errors403 == 1L, s.failures == 1L,
          state.outcomes == Vector(true)
        )
      }
    },
    test("IOException triggers connection retry and succeeds") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _, statsRef) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).flatMap { n =>
              if (n == 0) ZIO.fail(java.io.IOException("Connection reset"))
              else ZIO.succeed(Response.json(jsonBody))
            }
          }
          result <- client.get[Payload](testUrl)
          count  <- counter.get
          s      <- statsRef.get
          // 1 request, 2 attempts (first IOException, second succeeds)
        } yield assertTrue(
          result.value == "ok", count == 2,
          s.requests == 1L, s.attempts == 2L, s.connectionErrors == 1L, s.successes == 1L, s.failures == 0L
        )
      }
    },
    test("PrematureChannelClosureException triggers connection retry and succeeds") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _, statsRef) <- makeClient { _ =>
            counter.getAndUpdate(_ + 1).flatMap { n =>
              if (n == 0) ZIO.fail(PrematureChannelClosureException("Channel closed"))
              else ZIO.succeed(Response.json(jsonBody))
            }
          }
          result <- client.get[Payload](testUrl)
          count  <- counter.get
          s      <- statsRef.get
          // 1 request, 2 attempts (first PrematureChannelClosure, second succeeds)
        } yield assertTrue(
          result.value == "ok", count == 2,
          s.requests == 1L, s.attempts == 2L, s.connectionErrors == 1L, s.successes == 1L, s.failures == 0L
        )
      }
    },
    test("sustained connection errors trigger throttle-down") {
      ZIO.scoped {
        for {
          (client, stateRef, _) <- makeClient(
            handler = _ => ZIO.fail(java.io.IOException("Connection reset")),
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
    test("max-429-retries = 0 means no retries on 429") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _, statsRef) <- makeClient(
            handler = _ => counter.getAndUpdate(_ + 1).as(Response(status = Status.TooManyRequests)),
            max429Retries = 0
          )
          _     <- client.get[Payload](testUrl).exit
          count <- counter.get
          s     <- statsRef.get
          // max429Retries=0 → exactly 1 attempt, 0 retries
        } yield assertTrue(count == 1, s.attempts == 1L, s.errors429 == 1L, s.failures == 1L)
      }
    },
    test("non-transient non-HTTP error is not retried") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
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
            (client, stateRef, _) <- makeClient(
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
          (client, _, _) <- makeClient(
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
          (client, _, _) <- makeClient(
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
    suiteTimingStats,
    suiteStatsAccumulator,
    suitePersistStats
  ).provideShared(
    FreshSchemaLayer("test_client", Tables.ensureTables)
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.seconds)

  // ==========================================================================
  // Timing stats (integration)
  // ==========================================================================

  private def suiteTimingStats = suite("timing stats")(
    test("successful requests populate gate wait, EMA delay, and latency") {
      ZIO.scoped {
        for {
          (client, _, statsRef) <- makeClient(
            handler = _ => ZIO.sleep(5.millis).as(Response.json(jsonBody)),
            permits = 2
          )
          urls = (1 to 5).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          _    <- client.getAll[Payload](urls)
          s    <- statsRef.get
        } yield assertTrue(
          s.successes == 5L,
          s.gateWaitMs >= 0L,
          s.emaDelayMs >= 0L,
          s.latencyCount == 5L,
          s.latencyMinMs > 0L
        )
      }
    },
    test("throttle-down and recovery records throttled duration") {
      ZIO.scoped {
        for {
          shouldFail <- Ref.make(true)
          (client, _, statsRef) <- makeClient(
            handler = { _ =>
              shouldFail.get.map { fail =>
                if (fail) Response(status = Status.TooManyRequests)
                else Response.json(jsonBody)
              }
            },
            permits = 20,
            cooldown = 100.millis,
            failureThreshold = 0.2
          )
          // Trigger throttle-down
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          // Switch to success and drive recovery
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(6 to 30)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          _ <- ZIO.sleep(1.second)
          s <- statsRef.get
        } yield assertTrue(
          s.throttleDowns >= 1L,
          s.throttledMs > 0L
        )
      }
    }
  )

  // ==========================================================================
  // StatsAccumulator (pure)
  // ==========================================================================

  private def suiteStatsAccumulator = suite("StatsAccumulator")(
    test("incError routes 403 to errors403 (non-CF)") {
      val s = ChessComClient.StatsAccumulator().incError(403)
      assertTrue(s.errors403 == 1L, s.errorsCf403 == 0L, s.errors429 == 0L, s.errors404 == 0L)
    },
    test("incCf403 routes to errorsCf403 only") {
      val s = ChessComClient.StatsAccumulator().incCf403
      assertTrue(s.errorsCf403 == 1L, s.errors403 == 0L, s.errors429 == 0L, s.errors404 == 0L)
    },
    test("incError routes 404 to errors404") {
      val s = ChessComClient.StatsAccumulator().incError(404)
      assertTrue(s.errors404 == 1L, s.errors429 == 0L, s.errors403 == 0L, s.errorsCf403 == 0L)
    },
    test("incError ignores 429 and other status codes (tier-aware path enforced)") {
      // incError must not handle 429 — production code goes through incError429AtTier.
      val s429 = ChessComClient.StatsAccumulator().incError(429)
      val s500 = ChessComClient.StatsAccumulator().incError(500)
      assertTrue(
        s429.errors429 == 0L, s429.errors429ByTier.isEmpty,
        s500.errors429 == 0L, s500.errors403 == 0L, s500.errorsCf403 == 0L, s500.errors404 == 0L
      )
    },
    test("recordLatency tracks min, max, and histogram buckets") {
      val s = ChessComClient.StatsAccumulator()
        .recordLatency(30)   // bucket 0: 0-50
        .recordLatency(75)   // bucket 1: 50-100
        .recordLatency(150)  // bucket 2: 100-200
        .recordLatency(300)  // bucket 3: 200-500
        .recordLatency(800)  // bucket 4: 500-1000
        .recordLatency(1500) // bucket 5: 1000+
      assertTrue(
        s.latencyMinMs == 30L,
        s.latencyMaxMs == 1500L,
        s.latencySumMs == 2855L,
        s.latencyCount == 6L,
        s.latencyBuckets == Vector(1L, 1L, 1L, 1L, 1L, 1L)
      )
    },
    test("recordLatency assigns boundary values to lower bucket") {
      // Exactly 50 is NOT < 50, so it goes to the 50-100 bucket
      val s = ChessComClient.StatsAccumulator()
        .recordLatency(50)
        .recordLatency(100)
        .recordLatency(1000)
      assertTrue(
        s.latencyBuckets == Vector(0L, 1L, 1L, 0L, 0L, 1L)
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
    },
    test("addActiveMs accumulates total") {
      val s = ChessComClient.StatsAccumulator().addActiveMs(100).addActiveMs(200)
      assertTrue(s.activeMs == 300L)
    },
    test("deltaFrom subtracts additive counters and preserves window fields") {
      val prev = ChessComClient.StatsAccumulator(
        requests = 10, successes = 8, failures = 2, attempts = 15,
        errors429 = 3, errors403 = 1, errorsCf403 = 2, errors404 = 5, connectionErrors = 1,
        throttleDowns = 1, gateWaitMs = 100, emaDelayMs = 50, activeMs = 500,
        throttledMs = 200, latencySumMs = 1000, latencyCount = 10,
        latencyBuckets = Vector(2L, 3L, 2L, 1L, 1L, 1L)
      )
      val current = ChessComClient.StatsAccumulator(
        requests = 25, successes = 20, failures = 5, attempts = 35,
        errors429 = 7, errors403 = 3, errorsCf403 = 5, errors404 = 12, connectionErrors = 3,
        throttleDowns = 2, peakConcurrent = 6,
        latencyMinMs = 20, latencyMaxMs = 800,
        latencySumMs = 3000, latencyCount = 25,
        latencyBuckets = Vector(5L, 8L, 5L, 3L, 2L, 2L),
        gateWaitMs = 300, emaDelayMs = 120, activeMs = 1200,
        throttledMs = 500
      )
      val delta = current.deltaFrom(prev)
      assertTrue(
        delta.requests == 15L, delta.successes == 12L, delta.failures == 3L, delta.attempts == 20L,
        delta.errors429 == 4L, delta.errors403 == 2L, delta.errorsCf403 == 3L,
        delta.errors404 == 7L, delta.connectionErrors == 2L,
        delta.throttleDowns == 1L, delta.gateWaitMs == 200L, delta.emaDelayMs == 70L, delta.activeMs == 700L,
        delta.throttledMs == 300L, delta.latencySumMs == 2000L, delta.latencyCount == 15L,
        delta.latencyBuckets == Vector(3L, 5L, 3L, 2L, 1L, 1L),
        // Window fields taken from current, not subtracted
        delta.peakConcurrent == 6, delta.latencyMinMs == 20L, delta.latencyMaxMs == 800L
      )
    },
    test("ThrottleConfig.nextTier walks up the recovery ladder") {
      val cfg = ChessComClient.ThrottleConfig(
        Vector(2, 4, 6, 8), 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10
      )
      assertTrue(
        cfg.maxPermits == 8L,
        cfg.nextTier(1) == 2L,
        cfg.nextTier(2) == 4L,
        cfg.nextTier(4) == 6L,
        cfg.nextTier(6) == 8L,
        cfg.nextTier(8) == 8L,  // at top, stays at top
        cfg.nextTier(3) == 4L   // between tiers, goes to next
      )
    },
    test("ThrottleConfig rejects invalid recovery tiers") {
      val cases = List(
        Vector.empty[Int],         // empty
        Vector(1, 2, 4),           // contains 1
        Vector(4, 2, 8),           // not sorted
        Vector(2, 2, 4)            // duplicates
      )
      assertTrue(cases.forall { tiers =>
        scala.util.Try(
          ChessComClient.ThrottleConfig(
            tiers, 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10
          )
        ).isFailure
      })
    },
    test("ThrottleConfig rejects invalid timing and window values") {
      val tiers = Vector(2, 4, 8)
      def cfg(
        cooldown: Duration = 30.seconds,
        cfCooldown: Duration = 5.seconds,
        retryBase: Duration = 1.second,
        cfRetryDelay: Duration = 10.seconds,
        connectionRetryBase: Duration = 1.second,
        max429Retries: Int = 5,
        maxCfRetries: Int = 2,
        maxConnectionRetries: Int = 3,
        failureWindowSize: Int = 20,
        failureThreshold: Double = 0.2,
        minSampleSize: Int = 10
      ) = scala.util.Try(ChessComClient.ThrottleConfig(
        tiers, cooldown, cfCooldown, retryBase, cfRetryDelay, connectionRetryBase,
        max429Retries, maxCfRetries, maxConnectionRetries,
        failureWindowSize, failureThreshold, minSampleSize
      ))
      assertTrue(
        cfg(cooldown = (-1).seconds).isFailure,
        cfg(cfCooldown = (-1).seconds).isFailure,
        cfg(retryBase = 0.seconds).isFailure,         // must be strictly positive
        cfg(retryBase = (-1).seconds).isFailure,
        cfg(cfRetryDelay = (-1).seconds).isFailure,
        cfg(connectionRetryBase = 0.seconds).isFailure,  // must be strictly positive
        cfg(connectionRetryBase = (-1).seconds).isFailure,
        cfg(max429Retries = -1).isFailure,
        cfg(maxCfRetries = -1).isFailure,
        cfg(maxConnectionRetries = -1).isFailure,
        cfg(failureWindowSize = 0).isFailure,
        cfg(failureWindowSize = -1).isFailure,
        cfg(minSampleSize = 0).isFailure,
        cfg(minSampleSize = 25).isFailure,            // > windowSize (20)
        cfg(failureThreshold = -0.01).isFailure,
        cfg(failureThreshold = 1.01).isFailure,
        // Edge cases that SHOULD be accepted
        cfg(cooldown = 0.seconds).isSuccess,          // 0 cooldown = immediate recovery
        cfg(failureThreshold = 0.0).isSuccess,
        cfg(failureThreshold = 1.0).isSuccess,
        cfg(minSampleSize = 20).isSuccess             // equal to windowSize
      )
    },
    test("incAttemptAtTier increments both total and per-tier counters") {
      val s = ChessComClient.StatsAccumulator()
        .incAttemptAtTier(8)
        .incAttemptAtTier(8)
        .incAttemptAtTier(4)
        .incAttemptAtTier(1)
      assertTrue(
        s.attempts == 4L,
        s.attemptsByTier == Map(8 -> 2L, 4 -> 1L, 1 -> 1L)
      )
    },
    test("incError429AtTier increments both errors429 and per-tier map") {
      val s = ChessComClient.StatsAccumulator()
        .incError429AtTier(8)
        .incError429AtTier(8)
        .incError429AtTier(4)
      assertTrue(
        s.errors429 == 3L,
        s.errors429ByTier == Map(8 -> 2L, 4 -> 1L)
      )
    },
    test("deltaFrom subtracts per-tier maps element-wise") {
      val prev = ChessComClient.StatsAccumulator(
        attempts = 10, attemptsByTier = Map(8 -> 5L, 4 -> 3L, 2 -> 2L),
        errors429 = 3, errors429ByTier = Map(8 -> 2L, 4 -> 1L)
      )
      val current = ChessComClient.StatsAccumulator(
        attempts = 20, attemptsByTier = Map(8 -> 10L, 4 -> 5L, 2 -> 3L, 1 -> 2L),
        errors429 = 6, errors429ByTier = Map(8 -> 3L, 4 -> 2L, 1 -> 1L)
      )
      val delta = current.deltaFrom(prev)
      assertTrue(
        delta.attempts == 10L,
        delta.attemptsByTier == Map(8 -> 5L, 4 -> 2L, 2 -> 1L, 1 -> 2L),
        delta.errors429 == 3L,
        delta.errors429ByTier == Map(8 -> 1L, 4 -> 1L, 1 -> 1L)
      )
    },
    test("serializeTierMap produces sorted pipe-delimited string") {
      assertTrue(
        ChessComClient.StatsAccumulator.serializeTierMap(Map(8 -> 5L, 2 -> 3L, 4 -> 1L)) == "2:3|4:1|8:5",
        ChessComClient.StatsAccumulator.serializeTierMap(Map.empty) == ""
      )
    },
    test("subtractMaps drops zero-delta entries") {
      // Tier 8 had 5 attempts in prev and 5 in curr → delta 0, should be dropped
      assertTrue(
        ChessComClient.StatsAccumulator.subtractMaps(
          Map(8 -> 5L, 4 -> 3L), Map(8 -> 5L, 4 -> 1L)
        ) == Map(4 -> 2L)
      )
    },
    test("resetWindowFields resets peak, min, max, and buckets") {
      val s = ChessComClient.StatsAccumulator(
        requests = 10, successes = 8, peakConcurrent = 5,
        latencyMinMs = 30, latencyMaxMs = 500,
        latencyBuckets = Vector(1L, 2L, 3L, 2L, 1L, 1L)
      )
      val reset = s.resetWindowFields
      assertTrue(
        reset.requests == 10L, reset.successes == 8L,
        reset.peakConcurrent == 0,
        reset.latencyMinMs == Long.MaxValue, reset.latencyMaxMs == 0L,
        reset.latencyBuckets == Vector(0L, 0L, 0L, 0L, 0L, 0L)
      )
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
      prevSnapshot  <- Ref.make(ChessComClient.StatsAccumulator())
      prevFlushTime <- Ref.make(Instant.now())
      configIdRef   <- Ref.make(Option.empty[Long])
      stateRef      <- Ref.make(ChessComClient.ThrottleState(8, 0, Vector.empty))
      startedAt     <- ZIO.succeed(Instant.now())
      config = ChessComClient.ThrottleConfig(Vector(2, 4, 8), 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10)
    } yield ChessComClient.FlushContext(
      s"test-$appLabel", appLabel, startedAt, statsRef, prevSnapshot, prevFlushTime, configIdRef, config, stateRef, pgClient
    )

  private def suitePersistStats = suite("persistStats")(
    test("each flush inserts a new delta row") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(
          ChessComClient.StatsAccumulator().copy(
            requests = 5, successes = 4, failures = 1, activeMs = 500,
            attemptsByTier = Map(8 -> 6L, 4 -> 2L),
            errors429ByTier = Map(8 -> 1L)
          )
        )
        ctx      <- makeFlushContext("test-delta", statsRef, pgClient)
        // First flush: insert config + first delta row
        _         <- ChessComClient.persistStats(ctx)
        configId1 <- ctx.configIdRef.get
        // Second flush after more requests: should insert a SECOND row
        _         <- statsRef.update(_.copy(
          requests = 12, successes = 11, activeMs = 1200,
          attemptsByTier = Map(8 -> 10L, 4 -> 5L),
          errors429ByTier = Map(8 -> 2L, 4 -> 1L)
        ))
        _         <- ChessComClient.persistStats(ctx)
        configId2 <- ctx.configIdRef.get
        recent    <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
        rows       = recent.filter(_.appLabel == "test-delta").sortBy(_.startedAt)
      } yield assertTrue(
        configId1.isDefined,
        configId2 == configId1,
        rows.size == 2,
        rows(0).requests == 5L,
        rows(0).successes == 4L,
        rows(0).activeMs == 500L,
        rows(1).requests == 7L,   // delta: 12 - 5
        rows(1).successes == 7L,  // delta: 11 - 4
        rows(1).activeMs == 700L, // delta: 1200 - 500
        rows(0).sessionId == rows(1).sessionId,
        rows(0).configId == configId1.get,
        rows(0).currentPermits == 8,  // stateRef initialized at 8 in makeFlushContext
        // Per-tier round-trip: window 1 carries the initial values, window 2 carries the delta
        rows(0).attemptsByTier == "4:2|8:6",
        rows(0).errors429ByTier == "8:1",
        rows(1).attemptsByTier == "4:3|8:4",  // delta: 4→3 more, 8→4 more
        rows(1).errors429ByTier == "4:1|8:1"  // delta: 4→1 new, 8→1 more
      )
    },
    test("flush resets window-level fields for next window") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(
          ChessComClient.StatsAccumulator().copy(requests = 3, successes = 3, activeMs = 300)
            .updatePeak(5).recordLatency(100).recordLatency(200)
        )
        ctx <- makeFlushContext("test-reset", statsRef, pgClient)
        _   <- ChessComClient.persistStats(ctx)
        // After flush, window fields should be reset
        s <- statsRef.get
      } yield assertTrue(
        s.peakConcurrent == 0,
        s.latencyMinMs == Long.MaxValue,
        s.latencyMaxMs == 0L,
        s.latencyBuckets == Vector(0L, 0L, 0L, 0L, 0L, 0L),
        // Additive counters preserved
        s.requests == 3L,
        s.successes == 3L
      )
    },
    test("ensureConfig deduplicates identical configs") {
      val cc = {
        val c = ClientConfig(0L, "", "2|4|99", 88, 77, 66, 55, 44, 5, 2, 3, 33, 0.5, 22)
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
        stats1   <- Ref.make(ChessComClient.StatsAccumulator().copy(requests = 3, successes = 3, activeMs = 300))
        ctx1     <- makeFlushContext("test-dedup-1", stats1, pgClient)
        _        <- ChessComClient.persistStats(ctx1)
        cid1     <- ctx1.configIdRef.get
        stats2   <- Ref.make(ChessComClient.StatsAccumulator().copy(requests = 7, successes = 7, activeMs = 700))
        ctx2     <- makeFlushContext("test-dedup-2", stats2, pgClient)
        _        <- ChessComClient.persistStats(ctx2)
        cid2     <- ctx2.configIdRef.get
      } yield assertTrue(
        cid1.isDefined,
        cid2 == cid1
      )
    },
    test("in-progress throttle does not double-count across flushes") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ChessComClient.StatsAccumulator().copy(requests = 5, successes = 5, activeMs = 500))
        ctx      <- makeFlushContext("test-ongoing-throttle", statsRef, pgClient)
        // Manually set an ongoing throttle that started "100s ago"
        nowMs <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
        _     <- ctx.stateRef.update(_.copy(
          currentMax = 1, coolingDown = true, throttledSince = Some(nowMs - 100_000)
        ))
        _ <- ChessComClient.persistStats(ctx)
        // More activity, throttle still ongoing (another ~50s later conceptually)
        _ <- statsRef.update(_.copy(requests = 10, successes = 10, activeMs = 1000))
        _ <- ctx.stateRef.update(_.copy(throttledSince = Some(nowMs - 150_000)))
        _ <- ChessComClient.persistStats(ctx)
        recent <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
        rows    = recent.filter(_.appLabel == "test-ongoing-throttle").sortBy(_.startedAt)
      } yield assertTrue(
        rows.size == 2,
        // Each row's throttledMs represents only the portion attributable to that window.
        // Row 0 covers the first ~100s of throttle, row 1 the next ~50s.
        // Total across rows should equal the full ongoing duration (~150s), not 250s from double-counting.
        rows(0).throttledMs >= 95_000L && rows(0).throttledMs <= 105_000L,
        rows(1).throttledMs >= 45_000L && rows(1).throttledMs <= 55_000L
      )
    },
    test("skips persist when no requests made") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ChessComClient.StatsAccumulator())
        ctx      <- makeFlushContext("test-noop", statsRef, pgClient)
        _      <- ChessComClient.persistStats(ctx)
        recent <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
      } yield assertTrue(
        recent.count(_.appLabel == "test-noop") == 0
      )
    }
  )
}
