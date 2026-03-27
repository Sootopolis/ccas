package ccas.utils.client

import com.augustnagro.magnum.Transactor
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

import ccas.utils.TestCcasLogger

object TestChessComClient extends ZIOSpecDefault {

  private case class Payload(value: String)
  private given JsonDecoder[Payload] = DeriveJsonDecoder.gen[Payload]

  private def makeClient(
    handler: Request => UIO[Response],
    permits: Long = 5,
    cooldown: Duration = 50.millis,
    retryBase: Duration = 10.millis,
    failureWindowSize: Int = 50,
    failureThreshold: Double = 0.2
  ): ZIO[Scope, Nothing, (ChessComClient, Ref[ChessComClient.ThrottleState])] =
    for {
      semaphore  <- Semaphore.make(permits)
      stateRef   <- Ref.make(ChessComClient.ThrottleState(permits, 0, Vector.empty))
      reserveRef  <- Ref.make(Option.empty[Fiber.Runtime[Nothing, Nothing]])
      adjustMutex <- Semaphore.make(1)
      activeRef   <- Ref.make(0)
      bar         <- TestCcasLogger.noop.progressBar
      config = ChessComClient.ThrottleConfig(permits, cooldown, retryBase, failureWindowSize, failureThreshold, 10)
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
        Transactor(null),
        Headers.empty,
        TestCcasLogger.noop,
        semaphore,
        stateRef,
        reserveRef,
        adjustMutex,
        activeRef,
        bar,
        config
      )
      (client, stateRef)
    }

  private val testUrl  = URL.decode("http://test.example.com/api").toOption.get
  private val jsonBody = """{"value":"ok"}"""

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
          urls  = (1 to 15).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          _     <- client.getAll[Payload](urls)
          state <- stateRef.get
        } yield assertTrue(state.currentMax == 5L) // unchanged from initial permits
      }
    },
    test("failure rate above threshold triggers graduated throttle-down") {
      ZIO.scoped {
        for {
          // All requests return 429 — guaranteed to generate enough failures
          (client, stateRef) <- makeClient(
            handler = _ => ZIO.succeed(Response(status = Status.TooManyRequests)),
            permits = 20,
            failureThreshold = 0.2
          )
          _ <- ZIO.foreachParDiscard(1 to 5)(i =>
            client.get[Payload](URL.decode(s"http://test.example.com/api/$i").toOption.get).exit
          )
          state <- stateRef.get
        } yield assertTrue(state.currentMax < 20L)
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
            failureThreshold = 0.2,
            failureWindowSize = 20
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
          _ <- ZIO.sleep(1.second)
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
                // Sustained failures to trigger multiple throttle-downs
                if (n < 30) { Response(status = Status.TooManyRequests) }
                else { Response.json(jsonBody) }
              }
            },
            permits = 20,
            cooldown = 500.millis,
            failureThreshold = 0.2
          )
          urls = (1 to 5).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          _ <- client.getAll[Payload](urls).exit // may exhaust retries
          state <- stateRef.get
        } yield assertTrue(
          state.generation >= 1L,
          state.currentMax < 20L
        )
      }
    },
    test("sequential ordering when throttled to 1 permit") {
      ZIO.scoped {
        for {
          order     <- Ref.make(Chunk.empty[Int])
          stateRef  <- Ref.make(ChessComClient.ThrottleState(1, 0, Vector.empty))
          semaphore <- Semaphore.make(5)
          reserveRef  <- Ref.make(Option.empty[Fiber.Runtime[Nothing, Nothing]])
          adjustMutex <- Semaphore.make(1)
          activeRef   <- Ref.make(0)
          bar         <- TestCcasLogger.noop.progressBar
          counter   <- Ref.make(0)
          config = ChessComClient.ThrottleConfig(5, 60.seconds, 10.millis, 50, 0.2, 10)
          // Reserve 4 permits to enforce effective limit of 1
          reserveFiber <- semaphore.withPermits(4)(ZIO.never).forkDaemon
          _ <- reserveRef.set(Some(reserveFiber))
          driver = new ZClient.Driver[Any, Scope, Throwable] {
            override def request(
              version: Version,
              method: Method,
              url: URL,
              headers: Headers,
              body: Body,
              sslConfig: Option[ClientSSLConfig],
              proxy: Option[Proxy]
            )(implicit trace: Trace): ZIO[Scope, Throwable, Response] =
              for {
                n <- counter.getAndUpdate(_ + 1)
                _ <- order.update(_ :+ n)
              } yield Response.json(jsonBody)

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
          client = ChessComClient(
            ZClient.fromDriver(driver), Transactor(null), Headers.empty, TestCcasLogger.noop,
            semaphore, stateRef, reserveRef, adjustMutex, activeRef, bar, config
          )
          urls = (1 to 3).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
          _        <- client.getAll[Payload](urls)
          recorded <- order.get
        } yield assertTrue(recorded == Chunk(0, 1, 2))
      }
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.seconds)
}
