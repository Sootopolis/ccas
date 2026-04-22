package ccas.utils.client

import com.augustnagro.magnum.sql
import ccas.utils.sql.PostgresClient
import io.netty.handler.codec.PrematureChannelClosureException
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

import ccas.analysis.tables.{ApiResponseBody, ApiResponseCache, ClientConfig, ClientStats, Tables}
import ccas.analysis.tables.subtypes.ApiResponseBodyId
import ccas.utils.TestCcasLogger
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

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
    maxConnectionRetries: Int = 3,
    minRequestDelayMs: Long = 0,
    minTierObservation: Duration = Duration.Zero
  ): ZIO[Scope & PostgresClient, Nothing, (ChessComClient, Ref[ChessComClient.ThrottleState], Ref[ChessComClient.StatsAccumulator])] =
    for {
      testScope     <- ZIO.service[Scope]
      pgClient      <- ZIO.service[PostgresClient]
      stateRef      <- Ref.make(ChessComClient.ThrottleState(permits, 0, Vector.empty))
      activeRef     <- Ref.make(0)
      rateLimitGate <- Semaphore.make(1)
      lastReqRef    <- Ref.make(0L)
      bar                 <- TestCcasLogger.noopBar
      stats               <- Ref.make(ChessComClient.StatsAccumulator())
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
        10,
        minRequestDelayMs,
        minTierObservation
      )
      refs = ChessComClient.ThrottleRefs(
        stateRef,
        activeRef,
        rateLimitGate,
        lastReqRef
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
          // Poll until first recovery step fires
          _ <- stateRef.get.repeatUntil(_.currentMax == 3L)
            .timeoutFail(new RuntimeException("recovery did not reach tier 3"))(5.seconds)
        } yield assertTrue(
          throttled.currentMax == 1L
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
          // Phase 3: Poll until recovery advances past the throttled level
          _ <- stateRef.get.repeatUntil(_.currentMax > throttledMax)
            .timeoutFail(new RuntimeException("recovery did not advance"))(5.seconds)
        } yield assertTrue(
          throttledMax < 20L
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
          s.errorsCf403 == 3L, s.failures == 1L
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
          count == 1, s.requests == 1L, s.attempts == 1L, s.failures == 1L,
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
    test("connection errors do not trigger throttle-down") {
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
        } yield assertTrue(state.currentMax == 20L, !state.coolingDown)
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
        // Create client and trigger throttle-down inside a scope, then let the scope close.
        // Short cooldown is deliberate: once Phase 1 stops feeding failures, an uninterrupted
        // fiber needs ~2 cycles to advance (cycle 1 clears stale Phase-1 outcomes, cycle 2 sees
        // an empty window and steps up). The earlier cooldown=60s made this test vacuous — the
        // fiber was still in cooldown sleep for the full post-scope wait whether interrupted or not.
        (stateRef, insideState) <- ZIO.scoped {
          for {
            (client, stateRef, _) <- makeClient(
              handler = _ => ZIO.succeed(Response(status = Status.TooManyRequests)),
              permits = 20,
              cooldown = 100.millis,
              failureThreshold = 0.2
            )
            _ <- ZIO.foreachParDiscard(1 to 5)(i =>
              client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
            )
            insideState <- stateRef.get
          } yield (stateRef, insideState) // scope closes here, recovery fibers should be interrupted
        }
        // 1s >> drain-poll (200ms) + 2 × cooldown (200ms), so an uninterrupted fiber would have
        // stepped up through several tiers by now.
        _ <- ZIO.sleep(1.second)
        afterState <- stateRef.get
      } yield assertTrue(
        insideState.currentMax == 1L, // Phase 1 actually triggered throttle-down
        afterState.currentMax == 1L   // no post-scope step-up → fiber was interrupted
      )
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
    test("Cloudflare throttle during recovery drops to 1") {
      ZIO.scoped {
        for {
          mode <- Ref.make("429")
          (client, stateRef, _) <- makeClient(
            handler = { _ =>
              mode.get.map {
                case "429" => Response(status = Status.TooManyRequests)
                case "cf"  => Response(status = Status.Forbidden, body = Body.fromString(cfBody))
                case _     => Response.json(jsonBody)
              }
            },
            permits = 8,
            cooldown = 1.second,
            failureThreshold = 0.2,
            recoveryTiers = Some(Vector(3, 5, 8)),
            // Pins tier 3 long enough that polling can observe it before step 2 fires.
            minTierObservation = 3.seconds
          )
          // Phase 1: trigger failure-rate throttle → 1
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          // Phase 2: switch to success, flush window so recovery can advance
          _ <- mode.set("ok")
          _ <- ZIO.foreachDiscard(6 to 35)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Poll until first recovery step: tier 3, still cooling down
          midState <- stateRef.get.repeatUntil(_.currentMax >= 3L)
            .timeoutFail(new RuntimeException("recovery did not reach tier 3"))(10.seconds)
          gen1 = midState.generation
          // Phase 3: CF challenge overrides recovery (coolingDown is still true)
          _ <- mode.set("cf")
          _ <- client.get[Payload](URL.decode("http://test.example.com/api/cf").toOption.get).exit
          state <- stateRef.get
        } yield assertTrue(
          midState.currentMax == 3L,
          midState.coolingDown,
          state.currentMax == 1L,
          state.generation > gen1
        )
      }
    },
    test("coolingDown stays true during recovery ladder") {
      ZIO.scoped {
        for {
          shouldFail <- Ref.make(true)
          (client, stateRef, _) <- makeClient(
            handler = { _ =>
              shouldFail.get.map { fail =>
                if (fail) Response(status = Status.TooManyRequests)
                else Response.json(jsonBody)
              }
            },
            permits = 8,
            cooldown = 1.second,
            failureThreshold = 0.2,
            recoveryTiers = Some(Vector(2, 4, 8)),
            // Pins tier 2 long enough that polling can reliably observe it before step 2 fires
            // (same recipe as the "Cloudflare throttle during recovery drops to 1" test above).
            minTierObservation = 500.millis
          )
          // Trigger throttle-down → 1
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          // Switch to success and flush outcomes
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(6 to 35)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Poll until first recovery step: tier 2, still cooling down
          midState <- stateRef.get.repeatUntil(_.currentMax >= 2L)
            .timeoutFail(new RuntimeException("recovery did not reach tier 2"))(5.seconds)
          // Poll until full recovery: tier 8, coolingDown clears
          _ <- stateRef.get.repeatUntil(s => s.currentMax == 8L && !s.coolingDown)
            .timeoutFail(new RuntimeException("recovery did not complete"))(10.seconds)
        } yield assertTrue(
          midState.currentMax == 2L,
          midState.coolingDown
        )
      }
    },
    test("EMA resets to 0 on full recovery") {
      ZIO.scoped {
        for {
          shouldFail <- Ref.make(true)
          (client, stateRef, _) <- makeClient(
            handler = { _ =>
              shouldFail.get.flatMap { fail =>
                if (fail) ZIO.succeed(Response(status = Status.TooManyRequests))
                else ZIO.sleep(5.millis).as(Response.json(jsonBody))
              }
            },
            permits = 4,
            cooldown = 300.millis,
            failureThreshold = 0.2,
            recoveryTiers = Some(Vector(2, 4)),
            max429Retries = 0
          )
          // Trigger throttle-down (>= minSampleSize=10 outcomes needed, no retries)
          _ <- ZIO.foreachParDiscard(1 to 12)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          // Switch to success, build up EMA — requests finish well before recovery (2 x 300ms)
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(13 to 30)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          emaBefore <- stateRef.get.map(_.responseTimeEma)
          // No concurrent requests when recovery completes, so EMA=0 is observable
          _ <- stateRef.get.repeatUntil(s => s.currentMax == 4L && !s.coolingDown && s.responseTimeEma == 0.0)
            .timeoutFail(new RuntimeException("recovery did not complete with EMA reset"))(5.seconds)
        } yield assertTrue(
          emaBefore > 0.0
        )
      }
    },
    test("recovery drops back when new tier shows failures instead of advancing on stale outcomes") {
      ZIO.scoped {
        for {
          phase <- Ref.make(1)
          (client, stateRef, _) <- makeClient(
            handler = { _ =>
              phase.get.map {
                case 1 => Response(status = Status.TooManyRequests)
                case 2 => Response.json(jsonBody)
                case _ => Response(status = Status.TooManyRequests)
              }
            },
            permits = 8,
            cooldown = 500.millis,
            failureThreshold = 0.2,
            recoveryTiers = Some(Vector(2, 4, 8))
          )
          // Phase 1: trigger throttle-down
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          s1 <- stateRef.get
          // Phase 2: successes at tier 1
          _ <- phase.set(2)
          _ <- ZIO.foreachDiscard(6 to 25)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Wait for step-up to tier 2
          _ <- stateRef.get.repeatUntil(_.currentMax >= 2L)
            .timeoutFail(new RuntimeException("recovery did not reach tier 2"))(5.seconds)
          // Phase 3: 429s at tier 2 — since outcomes are cleared on step-up, only tier-2
          // failures are visible to the recovery check, causing a drop-back to 1
          _ <- phase.set(3)
          fiber <- ZIO.foreachParDiscard(26 to 45)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          ).fork
          _ <- stateRef.get.repeatUntil(_.currentMax <= 1L)
            .timeoutFail(new RuntimeException("recovery did not drop back to tier 1"))(5.seconds)
          _ <- fiber.interrupt
        } yield assertTrue(
          s1.currentMax == 1L
        )
      }
    },
    test("mid-recovery failures do not re-trigger throttle-down") {
      ZIO.scoped {
        for {
          counter <- Ref.make(0)
          (client, stateRef, statsRef) <- makeClient(
            handler = { _ =>
              counter.getAndUpdate(_ + 1).map { n =>
                // First batch: 429s to trigger throttle
                if (n < 15) Response(status = Status.TooManyRequests)
                // Mid-recovery: inject some 429s among successes
                else if (n >= 25 && n < 28) Response(status = Status.TooManyRequests)
                else Response.json(jsonBody)
              }
            },
            permits = 8,
            cooldown = 200.millis,
            failureThreshold = 0.2,
            recoveryTiers = Some(Vector(2, 4, 8))
          )
          // Phase 1: trigger throttle-down
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          s1 <- statsRef.get
          // Phase 2: successes with some 429s mixed in during recovery
          _ <- ZIO.foreachDiscard(16 to 40)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).catchAll(_ => ZIO.unit)
          )
          // Poll until recovery completes, then check no additional throttle-downs
          _ <- stateRef.get.repeatUntil(s => !s.coolingDown)
            .timeoutFail(new RuntimeException("recovery did not complete"))(5.seconds)
          s2 <- statsRef.get
        } yield assertTrue(
          s1.throttleDowns == 1L,
          s2.throttleDowns == 1L
        )
      }
    },
    test("min delay floor enforces minimum inter-request spacing") {
      ZIO.scoped {
        for {
          (client, _, statsRef) <- makeClient(
            handler = _ => ZIO.sleep(2.millis).as(Response.json(jsonBody)),
            permits = 2,
            minRequestDelayMs = 50
          )
          // Send requests sequentially so every request after the first hits the EMA floor
          _ <- ZIO.foreachDiscard(1 to 10)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          s <- statsRef.get
        } yield assertTrue(
          s.successes == 10L,
          s.emaDelayMs >= 200L // ~9 gaps at 50ms floor, conservatively at least 200ms total
        )
      }
    },
    test("min delay floor is inactive when maxPermits is 1") {
      ZIO.scoped {
        for {
          (client, _, _) <- makeClient(
            handler = _ => ZIO.succeed(Response.json(jsonBody)),
            permits = 1,
            minRequestDelayMs = 500
          )
          urls = (1 to 3).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          (duration, _) <- client.getAll[Payload](urls).timed
        } yield assertTrue(
          duration.toMillis < 1000 // floor not applied since maxPermits=1
        )
      }
    },
    test("min tier observation prevents premature recovery step-up") {
      ZIO.scoped {
        for {
          shouldFail <- Ref.make(true)
          (client, stateRef, _) <- makeClient(
            handler = { _ =>
              shouldFail.get.map { fail =>
                if (fail) Response(status = Status.TooManyRequests)
                else Response.json(jsonBody)
              }
            },
            permits = 8,
            cooldown = 50.millis,
            failureThreshold = 0.2,
            recoveryTiers = Some(Vector(2, 4, 8)),
            minTierObservation = 2.seconds
          )
          // Phase 1: trigger throttle-down
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          // Anchor the observation window to the scheduler's own tierEnteredAt rather than wall-clock
          // sleeps, so Phase 2 timing (which varies wildly under CI load) cannot push the observed
          // state past the step-up point before we assert on it.
          throttled <- stateRef.get
          throttleDownAt = throttled.tierEnteredAt.getOrElse(0L)
          // Phase 2: switch to success, fill outcome window so recovery has enough data
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(6 to 30)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Poll until step-up fires; elapsed from throttle-down must be at least minTierObservation
          _ <- stateRef.get.repeatUntil(_.currentMax >= 2L)
            .timeoutFail(new RuntimeException("step-up did not fire"))(5.seconds)
          stepUpAt <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
        } yield assertTrue(
          throttled.currentMax == 1L,
          (stepUpAt - throttleDownAt) >= 1800L // 2s observation - 200ms scheduling tolerance
        )
      }
    },
    test("tier observation resets on each recovery step") {
      ZIO.scoped {
        for {
          shouldFail <- Ref.make(true)
          (client, stateRef, _) <- makeClient(
            handler = { _ =>
              shouldFail.get.map { fail =>
                if (fail) Response(status = Status.TooManyRequests)
                else Response.json(jsonBody)
              }
            },
            permits = 4,
            cooldown = 50.millis,
            failureThreshold = 0.2,
            recoveryTiers = Some(Vector(2, 4)),
            minTierObservation = 1.second
          )
          // Trigger throttle-down
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(6 to 30)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Poll until step-up to tier 2. Capture the scheduler's own tierEnteredAt so polling drift
          // on the tier 2 side does not shrink the observed gap below the real observation window.
          tier2State <- stateRef.get.repeatUntil(_.currentMax >= 2L)
            .timeoutFail(new RuntimeException("did not reach tier 2"))(5.seconds)
          tier2EnteredAt = tier2State.tierEnteredAt.getOrElse(0L)
          // Poll until full recovery at tier 4 (top tier → coolingDown clears, tierEnteredAt is None).
          _ <- stateRef.get.repeatUntil(s => s.currentMax >= 4L && !s.coolingDown)
            .timeoutFail(new RuntimeException("did not reach tier 4"))(5.seconds)
          atTier4 <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
        } yield assertTrue(
          tier2State.currentMax == 2L, // sanity check: polling did not race past tier 2
          (atTier4 - tier2EnteredAt) >= 800L // 1s observation minus 200ms scheduling tolerance
        )
      }
    },
    suiteTimingStats,
    suiteStatsAccumulator,
    suitePersistStats,
    suiteCacheable
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
          (client, stateRef, statsRef) <- makeClient(
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
          // Poll until full recovery, which flushes throttledMs
          _ <- stateRef.get.repeatUntil(s => !s.coolingDown)
            .timeoutFail(new RuntimeException("recovery did not complete"))(5.seconds)
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
    test("incErrorOther increments errorsOther only") {
      val s = ChessComClient.StatsAccumulator().incErrorOther.incErrorOther
      assertTrue(s.errorsOther == 2L, s.errorsCf403 == 0L, s.errors429 == 0L)
    },
    test("incCf403AtTier increments both errorsCf403 and per-tier map") {
      val s = ChessComClient.StatsAccumulator()
        .incCf403AtTier(8)
        .incCf403AtTier(8)
        .incCf403AtTier(4)
      assertTrue(
        s.errorsCf403 == 3L,
        s.errorsCf403ByTier == Map(8 -> 2L, 4 -> 1L),
        s.errors429 == 0L,
        s.errorsOther == 0L
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
    test("ThrottleConfig.nextTier walks up the recovery ladder") {
      val cfg = ChessComClient.ThrottleConfig(
        Vector(2, 4, 6, 8), 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10, 0, Duration.Zero
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
    test("ThrottleConfig.previousTier walks down the recovery ladder") {
      val cfg = ChessComClient.ThrottleConfig(
        Vector(2, 4, 6, 8), 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10, 0, Duration.Zero
      )
      assertTrue(
        cfg.previousTier(8) == 6L,
        cfg.previousTier(6) == 4L,
        cfg.previousTier(4) == 2L,
        cfg.previousTier(2) == 1L,  // below first tier, falls to 1
        cfg.previousTier(1) == 1L,  // already at 1, stays at 1
        cfg.previousTier(5) == 4L   // between tiers, goes to previous
      )
    },
    test("ThrottleConfig rejects invalid recovery tiers") {
      val cases = List(
        Vector.empty[Int],         // empty
        Vector(0, 2, 4),           // contains 0
        Vector(4, 2, 8),           // not sorted
        Vector(2, 2, 4)            // duplicates
      )
      assertTrue(cases.forall { tiers =>
        scala.util.Try(
          ChessComClient.ThrottleConfig(
            tiers, 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10, 0, Duration.Zero
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
        minSampleSize: Int = 10,
        minRequestDelayMs: Long = 0,
        minTierObservation: Duration = Duration.Zero
      ) = scala.util.Try(ChessComClient.ThrottleConfig(
        tiers, cooldown, cfCooldown, retryBase, cfRetryDelay, connectionRetryBase,
        max429Retries, maxCfRetries, maxConnectionRetries,
        failureWindowSize, failureThreshold, minSampleSize,
        minRequestDelayMs, minTierObservation
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
        cfg(minRequestDelayMs = -1).isFailure,
        cfg(minTierObservation = (-1).seconds).isFailure,
        // Edge cases that SHOULD be accepted
        cfg(cooldown = 0.seconds).isSuccess,          // 0 cooldown = immediate recovery
        cfg(failureThreshold = 0.0).isSuccess,
        cfg(failureThreshold = 1.0).isSuccess,
        cfg(minSampleSize = 20).isSuccess,            // equal to windowSize
        cfg(minRequestDelayMs = 0).isSuccess,
        cfg(minTierObservation = Duration.Zero).isSuccess
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
    test("serializeTierMap produces sorted pipe-delimited string") {
      assertTrue(
        ChessComClient.StatsAccumulator.serializeTierMap(Map(8 -> 5L, 2 -> 3L, 4 -> 1L)) == "2:3|4:1|8:5",
        ChessComClient.StatsAccumulator.serializeTierMap(Map.empty) == ""
      )
    },
    test("ChessComClientConfig loads from test application.conf") {
      import ChessComClient.ChessComClientConfig
      import ChessComClient.ChessComClientConfig.*
      import zio.config.magnolia.DeriveConfig
      import zio.config.typesafe.TypesafeConfigProvider
      val provider = TypesafeConfigProvider.fromTypesafeConfig(
        com.typesafe.config.ConfigFactory.load(), enableCommaSeparatedValueAsList = true
      )
      for {
        cfg <- provider.load(summon[DeriveConfig[ChessComClientConfig]].desc.nested("chess-com-client"))
        tc  <- ZIO.attempt(cfg.toThrottleConfig)
      } yield assertTrue(
        cfg.contactEmail == "test@test.com",
        cfg.recoveryTiers == Vector(2, 4),
        cfg.cooldownSeconds == 1L,
        cfg.cfCooldownSeconds == 1L,
        cfg.failureWindowSize == 20,
        cfg.failureThreshold == 0.2,
        cfg.minSampleSize == 10,
        cfg.minRequestDelayMs == 0L,
        cfg.minTierObservationSeconds == 0L,
        cfg.retryBaseSeconds == 1L,
        cfg.cfRetryDelaySeconds == 1L,
        cfg.connectionRetryBaseSeconds == 1L,
        cfg.max429Retries == 2,
        cfg.maxCfRetries == 1,
        cfg.maxConnectionRetries == 2,
        cfg.statsFlushIntervalSeconds == 30L,
        tc.maxPermits == 4L,
        tc.cooldown == 1.second,
        tc.retryBase == 1.second
      )
    },
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
      configIdRef <- Ref.make(Option.empty[Long])
      stateRef    <- Ref.make(ChessComClient.ThrottleState(8, 0, Vector.empty))
      startedAt   <- ZIO.succeed(Instant.now())
      config = ChessComClient.ThrottleConfig(Vector(2, 4, 8), 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10, 0, Duration.Zero)
    } yield ChessComClient.FlushContext(
      s"test-$appLabel", appLabel, startedAt, statsRef, configIdRef, config, stateRef, pgClient
    )

  private def suitePersistStats = suite("persistStats")(
    test("repeated flushes upsert a single cumulative row") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(
          ChessComClient.StatsAccumulator().copy(
            requests = 5, successes = 4, failures = 1, activeMs = 500,
            attemptsByTier = Map(8 -> 6L, 4 -> 2L),
            errors429ByTier = Map(8 -> 1L)
          )
        )
        ctx      <- makeFlushContext("test-upsert", statsRef, pgClient)
        _         <- ChessComClient.persistStats(ctx)
        configId1 <- ctx.configIdRef.get
        // Second flush after more requests: should UPDATE the same row
        _         <- statsRef.update(_.copy(
          requests = 12, successes = 11, activeMs = 1200,
          attemptsByTier = Map(8 -> 10L, 4 -> 5L),
          errors429ByTier = Map(8 -> 2L, 4 -> 1L)
        ))
        _         <- ChessComClient.persistStats(ctx)
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
          ChessComClient.StatsAccumulator().copy(requests = 3, successes = 3, activeMs = 300)
            .updatePeak(5).recordLatency(100).recordLatency(200)
        )
        ctx <- makeFlushContext("test-no-mutate", statsRef, pgClient)
        _   <- ChessComClient.persistStats(ctx)
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
    test("in-progress throttle is included without mutating accumulator") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ChessComClient.StatsAccumulator().copy(requests = 5, successes = 5, activeMs = 500))
        ctx      <- makeFlushContext("test-ongoing-throttle", statsRef, pgClient)
        nowMs <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
        _     <- ctx.stateRef.update(_.copy(
          currentMax = 1, coolingDown = true, throttledSince = Some(nowMs - 100_000)
        ))
        _ <- ChessComClient.persistStats(ctx)
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
        statsRef <- Ref.make(ChessComClient.StatsAccumulator())
        ctx      <- makeFlushContext("test-noop", statsRef, pgClient)
        _      <- ChessComClient.persistStats(ctx)
        recent <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
      } yield assertTrue(
        recent.count(_.appLabel == "test-noop") == 0
      )
    },
    test("persists when cache-only activity (requests = 0, cacheHits > 0)") {
      for {
        pgClient <- ZIO.service[PostgresClient]
        statsRef <- Ref.make(ChessComClient.StatsAccumulator().copy(cacheHits = 3))
        ctx      <- makeFlushContext("test-cache-only", statsRef, pgClient)
        _      <- ChessComClient.persistStats(ctx)
        recent <- ClientStats.selectRecent(ctx.startedAt.minusSeconds(60))
        row     = recent.find(_.appLabel == "test-cache-only")
      } yield assertTrue(
        row.exists(_.requests == 0L),
        row.exists(_.cacheHits == 3L)
      )
    }
  )

  // ==========================================================================
  // Cache-aware dispatch via getCacheable — Fresh / Revalidated / IdenticalBody / Changed
  // ==========================================================================

  /** Build a `Response` with typed Cache-Control / ETag / Content-Type headers so the cache-parsing path is
    * exercised end-to-end. `maxAge = None` + `noStore = false` + `noCache = false` produces no Cache-Control header
    * at all. `noStore` or `noCache` combine with `maxAge` via `Header.CacheControl.Multiple` so the parser's
    * directive-walking path is also covered. `lastModified` is attached as a raw custom header so tests can inject
    * Chess.com's non-RFC date format verbatim and verify `HttpDate.parse` handles it.
    */
  private def cacheable200(
    body: String,
    maxAge: Option[Int] = Some(300),
    etag: Option[String] = Some("v1"),
    lastModified: Option[String] = None,
    noStore: Boolean = false,
    noCache: Boolean = false
  ): Response = {
    val directives: Vector[Header.CacheControl] =
      maxAge.map(n => Header.CacheControl.MaxAge(n)).toVector ++
        (if (noStore) Vector(Header.CacheControl.NoStore) else Vector.empty) ++
        (if (noCache) Vector(Header.CacheControl.NoCache) else Vector.empty)
    val ccHeader: Option[Header.CacheControl] = directives match {
      case Vector()  => None
      case Vector(d) => Some(d)
      case ds        => Some(Header.CacheControl.Multiple(NonEmptyChunk.fromIterable(ds.head, ds.tail)))
    }
    val etagHeader: Option[Header.ETag] = etag.map(Header.ETag.Strong(_))
    val allHeaders = Headers(Header.ContentType(MediaType.application.json)) ++
      ccHeader.fold(Headers.empty)(h => Headers(h)) ++
      etagHeader.fold(Headers.empty)(h => Headers(h)) ++
      lastModified.fold(Headers.empty)(lm => Headers(Header.Custom("Last-Modified", lm)))
    Response(status = Status.Ok, headers = allHeaders, body = Body.fromString(body))
  }

  /** Build a 304 Not Modified response with optional refreshed headers. Used by the metadata-refresh tests to
    * verify `handleNotModified` merges fresh `Cache-Control` / ETag / Last-Modified values from the 304 into the
    * stored cache entry.
    */
  private def notModified304(
    maxAge: Option[Int],
    etag: Option[String],
    lastModified: Option[String] = None,
    noCache: Boolean = false
  ): Response = {
    val directives: Vector[Header.CacheControl] =
      maxAge.map(n => Header.CacheControl.MaxAge(n)).toVector ++
        (if (noCache) Vector(Header.CacheControl.NoCache) else Vector.empty)
    val ccHeader: Option[Header.CacheControl] = directives match {
      case Vector()  => None
      case Vector(d) => Some(d)
      case ds        => Some(Header.CacheControl.Multiple(NonEmptyChunk.fromIterable(ds.head, ds.tail)))
    }
    val etagHeader: Option[Header.ETag] = etag.map(Header.ETag.Strong(_))
    val allHeaders = Headers.empty ++
      ccHeader.fold(Headers.empty)(h => Headers(h)) ++
      etagHeader.fold(Headers.empty)(h => Headers(h)) ++
      lastModified.fold(Headers.empty)(lm => Headers(Header.Custom("Last-Modified", lm)))
    Response(status = Status.NotModified, headers = allHeaders)
  }

  private def suiteCacheable = suite("cacheable dispatch")(
    test("first call returns Changed and populates cache") {
      val url = URL.decode("http://test.example.com/api/cacheable/changed-first").toOption.get
      ZIO.scoped {
        for {
          (client, _, stats) <- makeClient(_ => ZIO.succeed(cacheable200(jsonBody)))
          result <- client.getCacheable[Payload](url)
          value  <- result.getValue
          meta   <- ApiResponseCache.lookupMeta(url.encode)
          s      <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Changed[?]],
          !result.isUnchanged,
          value.value == "ok",
          meta.exists(_.etag.contains("\"v1\"")),
          meta.exists(_.maxAgeSeconds.contains(300L)),
          s.requests == 1L,
          s.cacheMisses == 1L,
          s.cacheHits == 0L,
          s.cacheRevalidations == 0L
        )
      }
    },
    test("second call within max-age returns Fresh without a network call") {
      val url = URL.decode("http://test.example.com/api/cacheable/fresh-hit").toOption.get
      ZIO.scoped {
        for {
          netCalls <- Ref.make(0)
          (client, _, stats) <- makeClient { _ =>
            netCalls.update(_ + 1).as(cacheable200(jsonBody, maxAge = Some(3600)))
          }
          _      <- client.getCacheable[Payload](url) // populate cache
          result <- client.getCacheable[Payload](url) // should Fresh
          value  <- result.getValue
          calls  <- netCalls.get
          s      <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Fresh[?]],
          result.isUnchanged,
          value.value == "ok",
          calls == 1, // only the first populate call hit the network
          s.requests == 1L,
          s.cacheHits == 1L
        )
      }
    },
    test("stale entry with 304 response returns Revalidated and touches fetched_at") {
      val url = URL.decode("http://test.example.com/api/cacheable/revalidated").toOption.get
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, stats) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(jsonBody, maxAge = Some(0)) // immediately stale on read
              else Response(status = Status.NotModified)
            }
          }
          _         <- client.getCacheable[Payload](url)
          before    <- ApiResponseCache.lookupMeta(url.encode)
          _         <- ZIO.sleep(20.millis) // ensure touched fetched_at differs
          result    <- client.getCacheable[Payload](url)
          value     <- result.getValue
          after     <- ApiResponseCache.lookupMeta(url.encode)
          s         <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Revalidated[?]],
          result.isUnchanged,
          value.value == "ok",
          before.exists(b => after.exists(_.fetchedAt.isAfter(b.fetchedAt))),
          s.cacheRevalidations == 1L,
          s.cacheHits == 0L,
          // 304 responses are attributed to the current permit tier in attemptsByTier, same as any other request —
          // the tier counter increments inside rawGet before the network call, so the 304 branch inherits it.
          s.attemptsByTier.values.sum == 2L // one populate attempt + one revalidation attempt
        )
      }
    },
    test("stale entry with 200 identical body returns IdenticalBody") {
      val url = URL.decode("http://test.example.com/api/cacheable/identical").toOption.get
      ZIO.scoped {
        for {
          // Always return the same body with a zero max-age so the cache row is always stale
          (client, _, stats) <- makeClient(_ => ZIO.succeed(cacheable200(jsonBody, maxAge = Some(0))))
          _      <- client.getCacheable[Payload](url) // populate cache
          result <- client.getCacheable[Payload](url) // server returns identical body
          value  <- result.getValue
          s      <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.IdenticalBody[?]],
          result.isUnchanged,
          value.value == "ok",
          s.requests == 2L,      // both calls hit the network
          s.cacheMisses == 1L,   // first call
          s.cacheHits == 1L      // second call — IdenticalBody increments cacheHits
        )
      }
    },
    test("stale entry with 200 different body returns Changed and replaces the cache row") {
      val url = URL.decode("http://test.example.com/api/cacheable/changed-on-revalidate").toOption.get
      val body1 = """{"value":"first"}"""
      val body2 = """{"value":"second"}"""
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, stats) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(body1, maxAge = Some(0), etag = Some("v1"))
              else cacheable200(body2, maxAge = Some(0), etag = Some("v2"))
            }
          }
          _            <- client.getCacheable[Payload](url)
          firstBodyId  <- ApiResponseCache.lookupMeta(url.encode).map(_.get.bodyId)
          result       <- client.getCacheable[Payload](url)
          value        <- result.getValue
          secondBodyId <- ApiResponseCache.lookupMeta(url.encode).map(_.get.bodyId)
          s            <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Changed[?]],
          !result.isUnchanged,
          value.value == "second",
          firstBodyId != secondBodyId,
          s.cacheMisses == 2L
        )
      }
    },
    test("Cache-Control: no-store response is not cached") {
      val url = URL.decode("http://test.example.com/api/cacheable/no-store").toOption.get
      ZIO.scoped {
        for {
          (client, _, _) <- makeClient(_ => ZIO.succeed(cacheable200(jsonBody, noStore = true)))
          result <- client.getCacheable[Payload](url)
          _      <- result.getValue
          meta   <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Changed[?]],
          meta.isEmpty
        )
      }
    },
    test("response without Cache-Control is never fresh (always revalidates or misses)") {
      val url = URL.decode("http://test.example.com/api/cacheable/no-cache-control").toOption.get
      ZIO.scoped {
        for {
          netCalls <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            netCalls.update(_ + 1).as(cacheable200(jsonBody, maxAge = None, etag = Some("only-etag")))
          }
          _      <- client.getCacheable[Payload](url) // populates cache without max-age
          result <- client.getCacheable[Payload](url) // should NOT be Fresh — server returned 200 with same body
          _      <- result.getValue
          calls  <- netCalls.get
        } yield assertTrue(
          // Without max-age the entry is never fresh; second call hits the network. With the same body returned,
          // ApiResponseBody dedupes by SHA-256 and we get IdenticalBody.
          result.isInstanceOf[CacheableResult.IdenticalBody[?]],
          calls == 2
        )
      }
    },
    test("Cache-Control: no-cache strips max-age so entries are always revalidated") {
      val url = URL.decode("http://test.example.com/api/cacheable/no-cache").toOption.get
      ZIO.scoped {
        for {
          netCalls <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            // Server sends no-cache alongside an hour-long max-age. Per RFC 7234 §5.2.2.2, the no-cache directive
            // wins: we must revalidate before reuse regardless of the max-age value.
            netCalls.update(_ + 1).as(cacheable200(jsonBody, maxAge = Some(3600), noCache = true))
          }
          _      <- client.getCacheable[Payload](url) // populates cache
          meta   <- ApiResponseCache.lookupMeta(url.encode)
          result <- client.getCacheable[Payload](url) // must hit the network despite max-age=3600
          _      <- result.getValue
          calls  <- netCalls.get
        } yield assertTrue(
          meta.exists(_.maxAgeSeconds.isEmpty), // no-cache stripped the max-age at persist time
          !result.isInstanceOf[CacheableResult.Fresh[?]],
          calls == 2
        )
      }
    },
    test("conditional request attaches If-None-Match in wire format (quotes preserved)") {
      val url = URL.decode("http://test.example.com/api/cacheable/if-none-match").toOption.get
      ZIO.scoped {
        for {
          lastHeaders <- Ref.make(Headers.empty)
          (client, _, _) <- makeClient { req =>
            lastHeaders.set(req.headers).as(cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc")))
          }
          _   <- client.getCacheable[Payload](url) // populate cache with etag
          _   <- client.getCacheable[Payload](url) // second call sends If-None-Match
          hs  <- lastHeaders.get
          inm = hs.rawHeader("If-None-Match")
        } yield assertTrue(
          // Chess.com expects quoted etag on the wire (RFC 7232). Bare "abc" would be rejected.
          inm.contains("\"abc\"")
        )
      }
    },
    test("Fresh whose body was pruned mid-flight falls through to a network refetch") {
      // Simulates a retention race: a caller receives CacheableResult.Fresh, holds the lazy load, and between
      // `lookupMeta` and `getValue` the body row gets deleted (e.g. by ApiResponseCache.deleteBefore on another
      // app's startup). loadAndDecode should treat the missing body as a cache miss and fetch fresh data.
      // Use a body that's unique to this test so api_response_body.deleteOrphans can actually remove it —
      // the shared jsonBody is referenced by cache rows from sibling tests and would be preserved.
      val url        = URL.decode("http://test.example.com/api/cacheable/race-delete").toOption.get
      val uniqueBody = """{"value":"race-delete-unique"}"""
      ZIO.scoped {
        for {
          netCalls <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            netCalls.update(_ + 1).as(cacheable200(uniqueBody, maxAge = Some(3600)))
          }
          _     <- client.getCacheable[Payload](url)               // populates cache (network call #1)
          fresh <- client.getCacheable[Payload](url)               // Fresh hit; body not loaded yet
          // Simulate the race: cascade-delete the cache row and its body row.
          _     <- ApiResponseCache.invalidate(url.encode)
          _     <- ApiResponseBody.deleteOrphans
          value <- fresh.getValue                                  // must recover via recursive get[T] (network #2)
          calls <- netCalls.get
        } yield assertTrue(
          fresh.isInstanceOf[CacheableResult.Fresh[?]],
          value.value == "race-delete-unique",
          calls == 2
        )
      }
    },
    test("Chess.com-format Last-Modified is parsed to an Instant and stored") {
      val url             = URL.decode("http://test.example.com/api/cacheable/lm-chess-com").toOption.get
      val chessComFormat  = "Thursday, 16-Apr-2026 23:13:22 GMT+0000"
      val expectedInstant = Instant.parse("2026-04-16T23:13:22Z")
      ZIO.scoped {
        for {
          (client, _, _) <- makeClient(_ => ZIO.succeed(
            cacheable200(jsonBody, lastModified = Some(chessComFormat))
          ))
          _    <- client.getCacheable[Payload](url)
          meta <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(meta.exists(_.lastModified.contains(expectedInstant)))
      }
    },
    test("conditional request sends If-Modified-Since in IMF-fixdate form regardless of received format") {
      val url            = URL.decode("http://test.example.com/api/cacheable/if-modified-since").toOption.get
      val chessComFormat = "Thursday, 16-Apr-2026 23:13:22 GMT+0000"
      ZIO.scoped {
        for {
          lastHeaders <- Ref.make(Headers.empty)
          (client, _, _) <- makeClient { req =>
            lastHeaders.set(req.headers).as(
              cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc"), lastModified = Some(chessComFormat))
            )
          }
          _   <- client.getCacheable[Payload](url)
          _   <- client.getCacheable[Payload](url)
          hs  <- lastHeaders.get
          ims = hs.rawHeader("If-Modified-Since")
        } yield assertTrue(
          ims.contains("Thu, 16 Apr 2026 23:13:22 GMT")
        )
      }
    },
    test("response without Last-Modified leaves column NULL and omits If-Modified-Since on revalidation") {
      val url = URL.decode("http://test.example.com/api/cacheable/lm-absent").toOption.get
      ZIO.scoped {
        for {
          lastHeaders <- Ref.make(Headers.empty)
          (client, _, _) <- makeClient { req =>
            lastHeaders.set(req.headers).as(cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc")))
          }
          _    <- client.getCacheable[Payload](url)
          _    <- client.getCacheable[Payload](url)
          meta <- ApiResponseCache.lookupMeta(url.encode)
          hs   <- lastHeaders.get
          ims   = hs.rawHeader("If-Modified-Since")
        } yield assertTrue(
          meta.exists(_.lastModified.isEmpty),
          ims.isEmpty
        )
      }
    },
    test("304 with Cache-Control: max-age=3600 overwrites the stored max_age_seconds") {
      val url = URL.decode("http://test.example.com/api/cacheable/304-max-age-bump").toOption.get
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc"))
              else notModified304(maxAge = Some(3600), etag = Some("abc"))
            }
          }
          _    <- client.getCacheable[Payload](url)
          _    <- client.getCacheable[Payload](url)
          meta <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(meta.exists(_.maxAgeSeconds.contains(3600L)))
      }
    },
    test("304 with Cache-Control: no-cache clears the stored max_age_seconds") {
      val url = URL.decode("http://test.example.com/api/cacheable/304-no-cache").toOption.get
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc"))
              else notModified304(maxAge = Some(3600), etag = Some("abc"), noCache = true)
            }
          }
          _    <- client.getCacheable[Payload](url)
          _    <- client.getCacheable[Payload](url)
          meta <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(meta.exists(_.maxAgeSeconds.isEmpty))
      }
    },
    test("304 with no refresh headers preserves stored metadata, only touches fetched_at") {
      val url             = URL.decode("http://test.example.com/api/cacheable/304-no-refresh").toOption.get
      val chessComFormat  = "Thursday, 16-Apr-2026 23:13:22 GMT+0000"
      val expectedInstant = Instant.parse("2026-04-16T23:13:22Z")
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc"), lastModified = Some(chessComFormat))
              else Response(status = Status.NotModified)
            }
          }
          _      <- client.getCacheable[Payload](url)
          before <- ApiResponseCache.lookupMeta(url.encode)
          _      <- ZIO.sleep(20.millis) // ensure touched fetched_at differs measurably
          _      <- client.getCacheable[Payload](url)
          after  <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(
          before.isDefined,
          after.isDefined,
          after.get.fetchedAt.isAfter(before.get.fetchedAt),
          after.get.etag == before.get.etag,
          after.get.lastModified.contains(expectedInstant),
          after.get.maxAgeSeconds == before.get.maxAgeSeconds,
          after.get.contentType == before.get.contentType
        )
      }
    },
    test("cached body that no longer parses (schema drift) triggers invalidate + refetch") {
      val url         = URL.decode("http://test.example.com/api/cacheable/schema-drift").toOption.get
      val beforeBody  = """{"value":"schema-drift-before"}"""
      val refetchBody = """{"value":"schema-drift-after"}"""
      ZIO.scoped {
        for {
          netCalls  <- Ref.make(0)
          stage     <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            stage.getAndUpdate(_ + 1).flatMap { s =>
              netCalls.update(_ + 1).as(
                // First call populates the cache with the original body; after the UPDATE-in-place below corrupts
                // that row, the recovery refetch should see a new body and replace the cache row.
                if (s == 0) cacheable200(beforeBody, maxAge = Some(3600))
                else cacheable200(refetchBody, maxAge = Some(3600))
              )
            }
          }
          _     <- client.getCacheable[Payload](url)
          fresh <- client.getCacheable[Payload](url)             // Fresh hit; body not loaded yet
          bodyId = fresh.asInstanceOf[CacheableResult.Fresh[Payload]].bodyId
          // Corrupt the cached body so the next decode throws JsonDecodingException. Unique body content means
          // this UPDATE only affects the row we created for this URL — no other test shares it.
          _     <- connectZIO(
            sql"UPDATE api_response_body SET body = '{\"oops\":true}' WHERE body_id = ${ApiResponseBodyId.unwrap(bodyId)}".update.run()
          )
          value <- fresh.getValue                                // decode fails → invalidate + refetch from network
          calls <- netCalls.get
          meta  <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(
          value.value == "schema-drift-after",
          calls == 2,                                            // first populate + recovery refetch
          meta.isDefined                                         // refetch re-populated the cache
        )
      }
    }
  )
}
