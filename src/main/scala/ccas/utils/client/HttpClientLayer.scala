package ccas.utils.client

import zio.{ZLayer, durationInt}
import zio.http.netty.NettyConfig
import zio.http.{Client, ConnectionPoolConfig, Decompression, DnsResolver, ZClient}

/** Custom zio-http Client ZLayer shared by every CCAS app and the server. Used in place of `Client.default` at every
  * layer-provisioning call site so that transport-level configuration (gzip, HTTP/2 in the future, connection pools,
  * etc.) is set in one place.
  *
  * '''Gzip''' — zio-http 3.10.1's `Client.default` uses `Decompression.No`, which means gzipped responses come back
  * as raw bytes and JSON decoding fails. We enable `Decompression.NonStrict` so Netty's `HttpContentDecompressor` is
  * installed in the pipeline and responses with `Content-Encoding: gzip` are decoded automatically. Responses that
  * are not compressed pass through unchanged, so the header-driven branching is entirely handled inside Netty.
  *
  * Note: `HttpContentDecompressor` strips `Content-Encoding` from the response after a successful decode, so no
  * runtime check in `ChessComClient` can tell a decompressed gzip response apart from an un-encoded one. Verify
  * manually when needed with `curl --compressed -v https://api.chess.com/pub/player/erik` and look for
  * `Content-Encoding: gzip` in the raw response headers. A silent regression in `Decompression.NonStrict` would
  * surface immediately as `JsonDecodingException` on every response (gzip bytes don't parse as JSON), and those
  * failures are already persisted to `api_fetch_failure` with dedup of the raw body into `api_response_body`.
  *
  * '''HTTP/2 (future)''' — zio-http 3.10.1 does not yet support HTTP/2; its `Version` sealed trait only defines
  * `Http_1_0` and `Http_1_1`, and the Netty client driver only negotiates HTTP/1.1 regardless of what ALPN offers.
  * HTTP/2 support is tracked upstream at https://github.com/zio/zio-http/issues/3473. When that lands in a zio-http
  * release we upgrade to, HTTP/2 becomes the single-line config change below (most likely a new field on
  * `ZClient.Config` such as `protocols` or `supportedVersions`). No other CCAS code needs to change: all 7 entry
  * points already flow through this layer, and `ChessComClient` never depends on the HTTP version — its batching
  * (`client.batched`), parallel `getAll` via `ZIO.foreachPar`, typed request/response headers, and adaptive throttle
  * are all protocol-version-agnostic. HTTP/2's connection multiplexing would transparently collapse our N parallel
  * TLS connections into a single multiplexed stream.
  */
object HttpClientLayer {

  val live: ZLayer[Any, Throwable, Client] = {
    val config = ZClient.Config.default
      .copy(requestDecompression = Decompression.NonStrict)
      // Transport-level timeouts so a silently-dropped TCP connection (peer gone, no RST/FIN) can't park a fiber
      // forever on a network read — the HTTP analogue of the DB `socketTimeout` hardening in `PostgresClient`.
      // `idleTimeout` (read/write inactivity) is pinned to zio-http 3.10.1's current 50s default rather than left
      // implicit, so a future zio-http upgrade can't silently remove this guard: it fires on a mid-response stall,
      // closes the channel, and fails the request, which `ChessComClient`'s connection-error retry then handles.
      // `connectionTimeout` defaults to `None` (unbounded connect) — set it so a black-holed SYN to the origin can't
      // hang a fiber on connection setup while waiting on the much longer OS-level TCP timeout.
      .idleTimeout(50.seconds)
      .connectionTimeout(10.seconds)
      // Connection pool with a `ttl` shorter than Chess.com's Cloudflare-edge idle timeout, so stale keep-alive
      // connections are evicted before the origin reaps them server-side. zio-http's default `Fixed(10)` has no idle
      // TTL, so it hands out reaped connections and the next request fails mid-flight with Netty's
      // `PrematureChannelClosureException` (`ChessComClient`'s connection-error retry then recovers, but noisily).
      // `maximum` is headroom over the real outbound concurrency ceiling (the gate's `maxPermits`, itself bounded by
      // `ApiConcurrency.fiberCap` ≤ the Hikari pool ~20), so raising `maxPermits` in config can't silently undersize
      // the pool below demand. `minimum = 0` keeps no idle connections, so the generous cap costs nothing at rest
      // (the pool drains during idle gaps — Neon scale-to-zero / between-job pauses).
      .copy(connectionPool = ConnectionPoolConfig.Dynamic(minimum = 0, maximum = 32, ttl = 30.seconds))
      // When zio-http adds HTTP/2 (issue #3473), enable it here, e.g.:
      //   .copy(protocols = NonEmptyChunk(Version.Http_2, Version.Http_1_1))
    (ZLayer.succeed(config) ++
      ZLayer.succeed(NettyConfig.defaultWithFastShutdown) ++
      DnsResolver.default) >>> Client.live
  }
}
