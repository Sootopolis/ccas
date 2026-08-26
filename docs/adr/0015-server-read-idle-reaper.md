# One global read-idle timeout on the server, and live follows get reaped by it

**Status:** Accepted, 2026-07-10 (#158, #161).

## Context

zio-http's `Server.Config.default` leaves `idleTimeout = None`. A client that vanishes without FIN or
RST therefore pins a Netty channel and its file descriptor forever — the server has no way to notice
the peer is gone.

zio-http exposes no per-route `idleTimeout` and no server-side `SO_KEEPALIVE` option, so one global
read-idle timeout is the only knob available.

## Decision

`Server.defaultWith(_.binding(host, port).idleTimeout(60.seconds))`, which installs a **read-only**
Netty `ReadTimeoutHandler` via `ServerChannelInitializer`: it resets on inbound client-to-server
reads only, never on writes.

## Consequences

- **Every live follow is reaped on the 60s schedule, by design.** A streaming
  `/api/jobs/{id}/{logs,progress}` follow is write-only server-to-client once the GET is sent, so
  `JobLogStream`'s keepalive ticks are outbound and cannot reset the timer. The follower reconnects
  transparently (#161), so the user sees nothing; the reaper's remaining real job is bounding idle
  **non-streaming** keep-alive descriptors.
- The reap surfaces as a `ReadTimeoutException` logged as "Fatal exception in Netty". It is benign
  noise and is filtered at the logger by `ProgressDisplay.isBenignReadIdleReap`. As with
  `NettyTailNoise` ([0005](0005-own-the-http-client-layer.md)), keep that match narrow — it is
  suppressing one known-harmless record, not a category.
- A shorter timeout would reap non-streaming connections sooner at the cost of more follow
  reconnects; a longer one trades the reverse. 60s is not load-bearing, but changing it changes the
  reconnect rate on every active follow, so measure that before tuning for descriptor pressure.
