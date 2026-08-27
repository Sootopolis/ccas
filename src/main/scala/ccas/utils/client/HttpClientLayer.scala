package ccas.utils.client

import zio.{ZIO, ZLayer, durationInt}
import zio.http.netty.NettyConfig
import zio.http.{Client, ConnectionPoolConfig, Decompression, DnsResolver, ZClient}

/** The zio-http `Client` layer every CCAS app and the server builds, in place of `Client.default`, so that
  * transport-level configuration lives in exactly one place.
  *
  * `Client.default` ships `Decompression.No`, which hands back gzipped bodies as raw bytes and fails every JSON
  * decode. This layer sets `Decompression.NonStrict`, bounds both network waits, and sizes the connection pool.
  *
  * Why each of those is set the way it is, how to verify gzip by hand, and what the pool's `ttl` does and does not
  * mean: `docs/adr/0005-own-the-http-client-layer.md` (#225).
  */
object HttpClientLayer {

  // A layer rather than an object-init side effect, so the install is tied to building a client; `ZIO.attempt` because
  // a log filter must never be able to fail the `Client` layer. Ordering is not load-bearing (see `install`).
  private val silenceTailNoise: ZLayer[Any, Nothing, Unit] =
    ZLayer(ZIO.attempt(NettyTailNoise.install()).catchAll(e => ZIO.logDebug(s"Netty tail-noise filter not installed: $e")))

  private val nettyClient: ZLayer[Any, Throwable, Client] = {
    val config = ZClient.Config.default
      .copy(requestDecompression = Decompression.NonStrict)
      // Bound both network waits so a silently-dropped TCP connection can't park a fiber forever. `idleTimeout` is
      // pinned to zio-http 3.11.4's own 50s default so a later upgrade can't drop the guard by changing it.
      .idleTimeout(50.seconds)
      .connectionTimeout(10.seconds)
      // `maximum` is headroom over the real outbound ceiling (`maxPermits`, itself bounded by
      // `ApiConcurrency.fiberCap`), so raising `maxPermits` can't undersize the pool. `minimum = 0` holds nothing
      // idle. `ttl` governs that at-rest drain, NOT per-connection idle expiry — see the ADR before changing it.
      .copy(connectionPool = ConnectionPoolConfig.Dynamic(minimum = 0, maximum = 32, ttl = 30.seconds))
    (ZLayer.succeed(config) ++
      ZLayer.succeed(NettyConfig.defaultWithFastShutdown) ++
      DnsResolver.default) >>> Client.live
  }

  val live: ZLayer[Any, Throwable, Client] = silenceTailNoise >>> nettyClient
}
