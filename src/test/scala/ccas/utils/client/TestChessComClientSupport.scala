package ccas.utils.client

import ccas.utils.sql.PostgresClient
import zio.{durationInt, RIO, Ref, Scope, Semaphore, ZIO}
import zio.http.*

import ccas.utils.TestCcasLogger

object TestChessComClientSupport {

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
      ema           <- Ref.make(0.0)
      bar           <- TestCcasLogger.noopBar
      stats         <- Ref.make(ChessComClient.StatsAccumulator())
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
        lastReqRef,
        ema
      )
      ChessComClient(
        ZClient.fromDriver(driver),
        pgClient,
        Headers.empty,
        TestCcasLogger.noop,
        refs,
        stats,
        bar,
        ChessComClient.ThrottleConfig(Vector(2, permits.toInt.max(2)).distinct, 30.seconds, 5.seconds, 1.second, 10.seconds, 1.second, 5, 2, 3, 20, 0.2, 10),
        Scope.global
      )
    }
}
