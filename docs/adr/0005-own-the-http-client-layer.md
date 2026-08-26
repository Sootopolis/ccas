# Own the zio-http client layer instead of using `Client.default`

**Status:** Accepted, 2026-04-16. Amended by #225 (pool `ttl` semantics, tail noise).

## Context

Every CCAS app and `CcasServer` needs a zio-http `Client`. Taking `Client.default` at each of the
seven provisioning sites would scatter transport configuration across the codebase and, more
immediately, ships defaults that are wrong for this workload.

## Decision

Build one `HttpClientLayer.live` and use it everywhere. It sets four things.

**Decompression.** `Client.default` uses `Decompression.No`, so a gzipped response arrives as raw
bytes and every JSON decode fails. We set `Decompression.NonStrict`, which installs Netty's
`HttpContentDecompressor`; uncompressed responses pass through unchanged, so all the branching
happens inside Netty.

`HttpContentDecompressor` strips `Content-Encoding` after a successful decode, so no runtime check
in `ChessComClient` can distinguish a decompressed gzip response from one that was never encoded.
Verify by hand instead: `curl --compressed -v https://api.chess.com/pub/player/erik` and look for
`Content-Encoding: gzip` in the raw headers. A regression here is not silent — gzip bytes do not
parse as JSON, so it surfaces as `JsonDecodingException` on every response, and those are already
persisted to `api_fetch_failure` with the raw body deduped into `api_response_body`.

**Transport timeouts.** A silently-dropped TCP connection — peer gone, no RST or FIN — otherwise
parks a fiber on a network read indefinitely; this is the HTTP analogue of the `socketTimeout`
hardening in `PostgresClient`. `idleTimeout` is pinned to zio-http 3.11.4's *current* 50s default
rather than left implicit, specifically so a future upgrade cannot remove the guard by changing its
default. It fires on a mid-response stall, closes the channel and fails the request, which
`ChessComClient`'s connection-error retry then handles. `connectionTimeout` defaults to `None`
(unbounded connect), so we set 10s: a black-holed SYN must not hang a fiber for the much longer
OS-level TCP timeout.

**Connection pool.** `maximum = 32` is headroom over the real outbound ceiling — the gate's
`maxPermits`, itself bounded by `ApiConcurrency.fiberCap` at roughly the Hikari pool size — so
raising `maxPermits` in config cannot silently undersize the pool below demand. `minimum = 0` keeps
no idle connections, so the generous cap costs nothing at rest.

**Tail-noise filter.** Installed as a layer rather than an object-init side effect, so it is tied to
building a client, and wrapped in `ZIO.attempt` because a log filter must never be able to fail the
`Client` layer.

Two exceptions share the class `PrematureChannelClosureException`, and only one means a request
failed. zio-http's ("Channel closed while executing the request…") is a real in-flight closure —
counted, written to `api_fetch_failure`, retried. Netty's ("channel gone inactive with N missing
response(s)") is an echo of that same failure, raised at the pipeline tail after `resetChannel`
removed `ClientFailureHandler`, the per-request handler that would have terminated `exceptionCaught`.
`NettyTailNoise` drops exactly that record.

Three choices there are load-bearing:

- **A JUL filter, not a `ZLogger` one.** Netty logs this itself, outside ZIO, so it prints straight
  through `ProgressDisplay`'s bars. `ProgressDisplay.isBenignReadIdleReap` is the same idea one layer
  up.
- **A `Filter`, not `setLevel(SEVERE)`.** `DefaultChannelPipeline` never logs at SEVERE, and on a
  channel idle in the pool both per-request handlers are gone, so the tail is the only place an
  `SSLException` or an `OutOfDirectMemoryError` could still surface.
- **The match is over-specified and fails _open_.** Level, exception type and the whole message must
  all hold, so a Netty reword brings the harmless noise back rather than hiding something real. Two
  near misses are kept deliberately: zio-http's identically-typed message, and any count above one,
  which no path we run should be able to reach.

Exit condition: zio-http hardcodes `HttpClientCodec(failOnMissingResponse = true)` in
`NettyConnectionPool`. Delete the filter when that changes (#225).

## Consequences

- `ttl` is **not** a per-connection idle expiry, whatever the name suggests. It reaches
  `ZPool.Strategy.TimeToLive`, whose clock resets on every allocate *and* release, so under a
  sustained crawl it never fires and cannot pre-empt a server-side reap. What it governs is the
  at-rest drain: how long after the last checkout the pool shrinks back to `minimum` (Neon
  scale-to-zero, pauses between jobs). Do not reach for the pool config to stop connection errors.
- Being handed a connection the origin has already closed is therefore unavoidable at this layer —
  the `channel.isOpen` check at checkout cannot see an unprocessed FIN — and is handled where it
  lands, by `ChessComClient`'s `retryConnectionSchedule`. Disabling keep-alive is not the answer
  either: a TLS handshake per request would land inside the pacing EMA's timed region (#216).
- HTTP/2 is unavailable in zio-http 3.11.4 — its `Version` sealed trait defines only `Http_1_0` and
  `Http_1_1`, and the Netty client driver negotiates HTTP/1.1 regardless of what ALPN offers.
  Tracked upstream at [zio/zio-http#3473](https://github.com/zio/zio-http/issues/3473). When it
  lands, this layer is the single point of change: no other CCAS code depends on the HTTP version.
