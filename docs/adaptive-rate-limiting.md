# Adaptive Rate Limiting for ChessComClient

## Problem

Chess.com's public API tolerates sequential requests but may return 429 (Too Many Requests) for parallel ones. The original `ChessComClient` used a static `Semaphore` — either fully parallel or fully sequential, with no ability to adapt at runtime or retry throttled requests.

## Options Considered

### 1. Static Semaphore (original)
- Single semaphore with configurable permits
- **Pros:** Simple
- **Cons:** No adaptation; must choose between fast-but-risky parallel or slow-but-safe sequential

### 2. Dynamic Semaphore Resize
- Shrink/grow permit count on 429/success
- **Pros:** Fine-grained concurrency control
- **Cons:** ZIO `Semaphore` doesn't support dynamic resizing; would require custom implementation

### 3. Token Bucket / Leaky Bucket
- Classic rate-limiting pattern with token replenishment
- **Pros:** Smooth rate limiting, well-understood
- **Cons:** Over-engineered for this use case; Chess.com doesn't publish rate limits, so bucket parameters would be guesswork

### 4. `Ref[Boolean]` + `Semaphore(1)` + `Schedule` (chosen)
- Start parallel (N-permit semaphore), switch to sequential (mutex) on 429, retry with backoff, resume parallel after cooldown
- **Pros:** Simple, adaptive, uses standard ZIO primitives, easy to test with `TestClock`
- **Cons:** Binary parallel/sequential toggle (no intermediate states)

## Chosen Approach: Option 4

### How It Works

1. **Normal mode:** Requests acquire the N-permit semaphore (parallel execution)
2. **On 429:** `rawGet` detects `Status.TooManyRequests`, calls `activateThrottle`, fails with `RateLimitedException`
3. **Throttled mode:** Requests acquire a `Semaphore(1)` mutex (sequential execution)
4. **Retry:** `get` wraps requests with exponential backoff retry (up to 4 retries, only on `RateLimitedException`)
5. **Cooldown:** A forked fiber resets `throttled` to `false` after 30 seconds (configurable)
6. **Resume:** After cooldown, requests return to parallel mode

### Key Design Decisions

- **`getAndSet` for cooldown:** Only the first 429 in a burst spawns a cooldown fiber; subsequent 429s during cooldown are no-ops
- **`foreachPar` in `getAll`:** The semaphore inside `get` naturally bounds concurrency, so `getAll` can safely use parallel iteration
- **Exponential backoff:** 1s base with doubling (1s, 2s, 4s, 8s) gives Chess.com time to recover
