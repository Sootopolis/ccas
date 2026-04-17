package ccas.utils.client

import zio.ZLayer
import zio.http.netty.NettyConfig
import zio.http.{Client, Decompression, DnsResolver, ZClient}

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
      // When zio-http adds HTTP/2 (issue #3473), enable it here, e.g.:
      //   .copy(protocols = NonEmptyChunk(Version.Http_2, Version.Http_1_1))
    (ZLayer.succeed(config) ++
      ZLayer.succeed(NettyConfig.defaultWithFastShutdown) ++
      DnsResolver.default) >>> Client.live
  }
}
