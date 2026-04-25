package ccas.utils.client

import io.netty.handler.codec.PrematureChannelClosureException
import zio.*
import zio.http.*
import zio.test.*

import ccas.analysis.tables.Tables
import ccas.utils.client.ChessComClient.ChessComClientConfig
import ccas.utils.client.ChessComClient.ChessComClientConfig.*
import ccas.utils.client.TestChessComClientSupport.*
import ccas.utils.sql.FreshSchemaLayer
import zio.config.magnolia.DeriveConfig
import zio.config.typesafe.TypesafeConfigProvider

object TestChessComClientThrottling extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] = suite("TestChessComClientThrottling")(
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
        // Cooldown must comfortably exceed Phase 1 + scope-cleanup duration: if it elapses
        // before scope close fires the interrupt finalizer, the recovery fiber races into
        // step-up and currentMax advances before the interrupt arrives. The earlier 100 ms
        // value flaked under CI load for this reason. Post-scope wait stays well above
        // cooldown so an uninterrupted fiber would still demonstrably step up.
        (stateRef, insideState) <- ZIO.scoped {
          for {
            (client, stateRef, _) <- makeClient(
              handler = _ => ZIO.succeed(Response(status = Status.TooManyRequests)),
              permits = 20,
              cooldown = 2.seconds,
              failureThreshold = 0.2
            )
            _ <- ZIO.foreachParDiscard(1 to 5)(i =>
              client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
            )
            insideState <- stateRef.get
          } yield (stateRef, insideState) // scope closes here, recovery fibers should be interrupted
        }
        // 4s = 2 × cooldown: an uninterrupted fiber would have cleared cooldown and stepped up.
        _ <- ZIO.sleep(4.seconds)
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
          // Measure actual inter-request spacing instead of emaDelayMs: the cumulative
          // counter only grows when emaDelay slept, but under load natural gaps can
          // already exceed the floor and the counter stays low even though the floor
          // is being respected.
          timestamps <- Ref.make(List.empty[Long])
          (client, _, statsRef) <- makeClient(
            handler = _ =>
              for {
                now <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
                _   <- timestamps.update(now :: _)
                _   <- ZIO.sleep(2.millis)
              } yield Response.json(jsonBody),
            permits = 2,
            minRequestDelayMs = 50
          )
          // Sequential so every request after the first goes through the floor check.
          _ <- ZIO.foreachDiscard(1 to 10)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get)
          )
          s   <- statsRef.get
          ts  <- timestamps.get.map(_.reverse)
          gaps = ts.sliding(2).collect { case List(a, b) => b - a }.toList
          // Skip request 2's gap: lastRequestRef is only set inside emaDelay's active branch
          // and request 1 doesn't enter that branch (ema starts at 0), so request 2's gap
          // is computed against an uninitialized lastRequestRef and bypasses the floor by
          // design. From request 3 onwards the floor is enforced on every iteration.
          flooredGaps = gaps.drop(1)
        } yield assertTrue(
          s.successes == 10L,
          ts.size == 10,
          flooredGaps.forall(_ >= 40L) // 50ms floor minus 10ms scheduler-jitter tolerance
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
    suiteStatsAccumulator
  ).provideShared(
    FreshSchemaLayer("test_client_throttling", Tables.ensureTables)
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
  // ClientStatsAccumulator (pure)
  // ==========================================================================

  private def suiteStatsAccumulator = suite("ClientStatsAccumulator")(
    test("incErrorOther increments errorsOther only") {
      val s = ClientStatsAccumulator().incErrorOther.incErrorOther
      assertTrue(s.errorsOther == 2L, s.errorsCf403 == 0L, s.errors429 == 0L)
    },
    test("incCf403AtTier increments both errorsCf403 and per-tier map") {
      val s = ClientStatsAccumulator()
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
      val s = ClientStatsAccumulator()
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
      val s = ClientStatsAccumulator()
        .recordLatency(50)
        .recordLatency(100)
        .recordLatency(1000)
      assertTrue(
        s.latencyBuckets == Vector(0L, 1L, 1L, 0L, 0L, 1L)
      )
    },
    test("updatePeak tracks maximum concurrent") {
      val s = ClientStatsAccumulator()
        .updatePeak(3)
        .updatePeak(1)
        .updatePeak(5)
        .updatePeak(2)
      assertTrue(s.peakConcurrent == 5)
    },
    test("addGateWait accumulates total") {
      val s = ClientStatsAccumulator().addGateWait(100).addGateWait(50)
      assertTrue(s.gateWaitMs == 150L)
    },
    test("addEmaDelay accumulates total") {
      val s = ClientStatsAccumulator().addEmaDelay(200).addEmaDelay(30)
      assertTrue(s.emaDelayMs == 230L)
    },
    test("addThrottled accumulates total") {
      val s = ClientStatsAccumulator().addThrottled(5000).addThrottled(3000)
      assertTrue(s.throttledMs == 8000L)
    },
    test("addActiveMs accumulates total") {
      val s = ClientStatsAccumulator().addActiveMs(100).addActiveMs(200)
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
      val s = ClientStatsAccumulator()
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
      val s = ClientStatsAccumulator()
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
        ClientStatsAccumulator.serializeTierMap(Map(8 -> 5L, 2 -> 3L, 4 -> 1L)) == "2:3|4:1|8:5",
        ClientStatsAccumulator.serializeTierMap(Map.empty) == ""
      )
    },
    test("ChessComClientConfig loads from test application.conf") {
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
}
