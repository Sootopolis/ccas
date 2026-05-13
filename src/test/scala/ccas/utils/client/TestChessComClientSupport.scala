package ccas.utils.client

import ccas.utils.sql.PostgresClient
import zio.{durationInt, Duration, RIO, Ref, Scope, Semaphore, Task, URLayer, ZIO, ZLayer}
import zio.http.*
import zio.json.*

import ccas.utils.ProgressDisplay

object TestChessComClientSupport {

  case class Payload(value: String)
  given JsonDecoder[Payload] = DeriveJsonDecoder.gen[Payload]

  val testUrl: URL  = URL.decode("http://test.example.com/api").toOption.get
  val jsonBody: String = """{"value":"ok"}"""
  val cfBody: String =
    """<html><head><title>Just a moment...</title></head><body><script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script></body></html>"""

  /** Generate doubling recovery tiers up to `max`, e.g. max=20 → Vector(2, 4, 8, 16, 20). */
  def doublingTiers(max: Int): Vector[Int] = {
    val base  = Iterator.iterate(2)(_ * 2).takeWhile(_ < max.max(2)).toVector
    val tiers = base :+ max.max(2)
    tiers.distinct
  }

  def makeClient(
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
    minTierObservation: Duration = Duration.Zero,
    emaTauMs: Long = 500
  ): ZIO[Scope & PostgresClient, Nothing, (ChessComClient, Ref[ChessComClient.ThrottleState], Ref[ClientStatsAccumulator])] =
    for {
      testScope     <- ZIO.service[Scope]
      pgClient      <- ZIO.service[PostgresClient]
      stateRef      <- Ref.make(ChessComClient.ThrottleState(permits, 0, Vector.empty))
      activeRef     <- Ref.make(0)
      rateLimitGate <- Semaphore.make(1)
      lastReqRef    <- Ref.make(0L)
      bar                 <- ProgressDisplay.make(enabled = false).addBar
      stats               <- Ref.make(ClientStatsAccumulator())
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
        minTierObservation,
        emaTauMs
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
        )(implicit trace: zio.Trace): ZIO[Scope, Throwable, Response] =
          handler(Request(method = method, url = url, headers = headers, body = body))

        override def socket[Env1 <: Any](
          version: Version,
          url: URL,
          headers: Headers,
          app: WebSocketApp[Env1]
        )(implicit
          trace: zio.Trace,
          ev: Scope =:= Scope
        ): ZIO[Env1 & Scope, Throwable, Response] =
          ZIO.die(new UnsupportedOperationException)
      }
      val client = ChessComClient(
        ZClient.fromDriver(driver),
        pgClient,
        Headers.empty,
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

  /** Build a [[ChessComClient]] backed by in-memory [[Routes]] instead of a real HTTP connection. Eliminates the
    * boilerplate of creating ThrottleRefs, a ZClient.Driver, and wiring them together that was previously duplicated
    * across test files.
    *
    * @param routes
    *   ZIO HTTP routes that serve as the fake API
    * @param permits
    *   max concurrent requests (default 1 — sequential, simplest for tests)
    */
  def fakeClient(
    routes: Routes[Any, Response],
    permits: Long = 1
  ): RIO[PostgresClient, ChessComClient] =
    for {
      pgClient      <- ZIO.service[PostgresClient]
      stateRef      <- Ref.make(ChessComClient.ThrottleState(permits, 0, Vector.empty))
      activeRef     <- Ref.make(0)
      rateLimitGate <- Semaphore.make(1)
      lastReqRef    <- Ref.make(0L)
      bar                 <- ProgressDisplay.make(enabled = false).addBar
      stats               <- Ref.make(ClientStatsAccumulator())
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
        )(implicit trace: zio.Trace): ZIO[Scope, Throwable, Response] =
          routes.runZIO(Request(method = method, url = url, headers = headers, body = body))

        override def socket[Env1 <: Any](
          version: Version,
          url: URL,
          headers: Headers,
          app: WebSocketApp[Env1]
        )(implicit
          trace: zio.Trace,
          ev: Scope =:= Scope
        ): ZIO[Env1 & Scope, Throwable, Response] =
          ZIO.die(new UnsupportedOperationException)
      }
      val refs = ChessComClient.ThrottleRefs(
        stateRef,
        activeRef,
        rateLimitGate,
        lastReqRef
      )
      ChessComClient(
        ZClient.fromDriver(driver),
        pgClient,
        Headers.empty,
        refs,
        stats,
        bar,
        ChessComClient.ThrottleConfig(Vector(2, permits.toInt.max(2)).distinct, 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10, 0, Duration.Zero, 500L),
        Scope.global
      )
    }
}
