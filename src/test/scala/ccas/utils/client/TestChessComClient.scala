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
        // Wait longer than the cooldown — if recovery were alive, it would advance permits
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
            recoveryTiers = Some(Vector(3, 5, 8))
          )
          // Phase 1: trigger failure-rate throttle → 1
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          // Phase 2: switch to success, flush window, let recovery advance to tier 3
          _ <- mode.set("ok")
          _ <- ZIO.foreachDiscard(6 to 35)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          _ <- ZIO.sleep(1200.millis)
          midState <- stateRef.get
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
            recoveryTiers = Some(Vector(2, 4, 8))
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
          // Phase 3: 429s at tier 2 — since outcomes are cleared on step-up, only tier-2
          // failures are visible to the recovery check, causing a drop-back to 1
          _ <- phase.set(3)
          fiber <- ZIO.foreachParDiscard(26 to 45)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          ).fork
          _ <- stateRef.get.repeatUntil(_.currentMax <= 1L)
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
          // Phase 2: switch to success, fill outcome window so recovery has enough data
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(6 to 30)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Cooldown is 50ms but observation is 2s — well before observation expires, should still be at tier 1
          _ <- ZIO.sleep(500.millis)
          earlyState <- stateRef.get
          // After observation period: should have stepped up
          _ <- ZIO.sleep(2.seconds)
          lateState <- stateRef.get
        } yield assertTrue(
          earlyState.currentMax == 1L,
          lateState.currentMax >= 2L
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
            minTierObservation = 300.millis
          )
          // Trigger throttle-down
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          _ <- shouldFail.set(false)
          _ <- ZIO.foreachDiscard(6 to 30)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          // Wait for first step-up to tier 2 (cooldown 50ms + observation 300ms)
          _ <- stateRef.get.repeatUntil(_.currentMax >= 2L)
            .timeoutFail(new RuntimeException("did not reach tier 2"))(2.seconds)
          // Record time at tier 2
          atTier2 <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
          // Wait for step-up to tier 4 (another observation period required)
          _ <- stateRef.get.repeatUntil(s => s.currentMax >= 4L && !s.coolingDown)
            .timeoutFail(new RuntimeException("did not reach tier 4"))(2.seconds)
          atTier4 <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
          // The gap between reaching tier 2 and tier 4 must be at least minTierObservation
        } yield assertTrue(
          (atTier4 - atTier2) >= 250L // 300ms observation minus scheduling tolerance
        )
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
    }
  )
}
