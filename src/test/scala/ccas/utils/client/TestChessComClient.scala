package ccas.utils.client

import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

object TestChessComClient extends ZIOSpecDefault {

  private case class Payload(value: String)
  private given JsonDecoder[Payload] = DeriveJsonDecoder.gen[Payload]

  private def makeClient(
    handler: Request => ZIO[Any, Nothing, Response],
    permits: Long = 5,
    cooldown: Duration = 30.seconds,
  ): ZIO[Any, Nothing, (ChessComClient, Ref[Boolean])] =
    for {
      semaphore <- Semaphore.make(permits)
      mutex     <- Semaphore.make(1)
      throttled <- Ref.make(false)
    } yield {
      val driver = new ZClient.Driver[Any, Scope, Throwable] {
        override def request(
          version: Version, method: Method, url: URL, headers: Headers, body: Body,
          sslConfig: Option[ClientSSLConfig], proxy: Option[Proxy],
        )(implicit trace: Trace): ZIO[Scope, Throwable, Response] =
          handler(Request(method = method, url = url, headers = headers, body = body))

        override def socket[Env1 <: Any](
          version: Version, url: URL, headers: Headers, app: WebSocketApp[Env1],
        )(implicit trace: Trace, ev: Scope =:= Scope): ZIO[Env1 & Scope, Throwable, Response] =
          ZIO.die(new UnsupportedOperationException)
      }
      val client = ChessComClient(ZClient.fromDriver(driver), Headers.empty, semaphore, mutex, throttled, cooldown)
      (client, throttled)
    }

  private val testUrl = URL.decode("http://test.example.com/api").toOption.get
  private val jsonBody = """{"value":"ok"}"""

  /** Fork a fiber that keeps advancing TestClock so ZIO.sleep calls resolve quickly. */
  private val advanceClock: ZIO[Live, Nothing, Fiber[Nothing, Nothing]] =
    (TestClock.adjust(1.second) *> Live.live(ZIO.sleep(1.millis))).forever.fork

  override def spec: Spec[TestEnvironment, Any] = suite("TestChessComClient")(
    test("normal 200 succeeds without throttle activation") {
      for {
        (client, _) <- makeClient(_ => ZIO.succeed(Response.json(jsonBody)))
        result      <- client.get[Payload](testUrl)
      } yield assertTrue(result.value == "ok")
    },

    test("429 triggers retry and succeeds on subsequent attempt") {
      for {
        counter     <- Ref.make(0)
        (client, _) <- makeClient { _ =>
          counter.getAndUpdate(_ + 1).map { n =>
            if n == 0 then Response(status = Status.TooManyRequests)
            else Response.json(jsonBody)
          }
        }
        clock  <- advanceClock
        result <- client.get[Payload](testUrl)
        _      <- clock.interrupt
        count  <- counter.get
      } yield assertTrue(result.value == "ok", count == 2)
    },

    test("429 sets throttled ref to true") {
      for {
        (client, throttled) <- makeClient(_ => ZIO.succeed(Response(status = Status.TooManyRequests)))
        clock               <- advanceClock
        _                   <- client.get[Payload](testUrl).exit
        _                   <- clock.interrupt
        isThrottled         <- throttled.get
      } yield assertTrue(isThrottled)
    },

    test("cooldown resets throttle") {
      for {
        counter             <- Ref.make(0)
        (client, throttled) <- makeClient(
          handler = { _ =>
            counter.getAndUpdate(_ + 1).map { n =>
              if n == 0 then Response(status = Status.TooManyRequests)
              else Response.json(jsonBody)
            }
          },
          cooldown = 30.seconds,
        )
        // Advance clock enough for the retry backoff (1s) so the get completes
        clock  <- advanceClock
        _      <- client.get[Payload](testUrl)
        _      <- clock.interrupt
        // After successful retry, throttle should be active
        throttledBefore <- throttled.get
        // Advance past cooldown
        _ <- TestClock.adjust(31.seconds)
        throttledAfter <- throttled.get
      } yield assertTrue(throttledBefore, !throttledAfter)
    },

    test("exhausted retries surface RateLimitedException") {
      for {
        (client, _) <- makeClient(_ => ZIO.succeed(Response(status = Status.TooManyRequests)))
        clock       <- advanceClock
        exit        <- client.get[Payload](testUrl).exit
        _           <- clock.interrupt
      } yield assertTrue(exit.isFailure)
    },

    test("sequential ordering when throttled") {
      for {
        order     <- Ref.make(Chunk.empty[Int])
        throttled <- Ref.make(true)
        semaphore <- Semaphore.make(5)
        mutex     <- Semaphore.make(1)
        counter   <- Ref.make(0)
        driver = new ZClient.Driver[Any, Scope, Throwable] {
          override def request(
            version: Version, method: Method, url: URL, headers: Headers, body: Body,
            sslConfig: Option[ClientSSLConfig], proxy: Option[Proxy],
          )(implicit trace: Trace): ZIO[Scope, Throwable, Response] =
            for {
              n <- counter.getAndUpdate(_ + 1)
              _ <- order.update(_ :+ n)
            } yield Response.json(jsonBody)

          override def socket[Env1 <: Any](
            version: Version, url: URL, headers: Headers, app: WebSocketApp[Env1],
          )(implicit trace: Trace, ev: Scope =:= Scope): ZIO[Env1 & Scope, Throwable, Response] =
            ZIO.die(new UnsupportedOperationException)
        }
        client = ChessComClient(ZClient.fromDriver(driver), Headers.empty, semaphore, mutex, throttled, 60.seconds)
        urls = (1 to 3).map(i => URL.decode(s"http://test.example.com/api/$i").toOption.get)
        _ <- client.getAll[Payload](urls)
        recorded <- order.get
      } yield assertTrue(recorded == Chunk(0, 1, 2))
    },
  )
}
