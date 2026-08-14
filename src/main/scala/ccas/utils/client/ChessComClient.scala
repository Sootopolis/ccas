package ccas.utils.client

import java.time.{Instant, ZoneOffset}

import ccas.utils.sql.PostgresClient
import com.typesafe.config.ConfigFactory
import zio.*
import zio.config.derivation.{kebabCase, name}
import zio.config.magnolia.DeriveConfig
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.*
import zio.http.Method.GET
import zio.json.JsonDecoder

import ccas.analysis.tables.{ApiFetchFailure, ApiResponseBody, ApiResponseCache}
import ccas.analysis.tables.subtypes.ApiResponseBodyId
import ccas.info.BuildInfo
import ccas.utils.{HttpDate, ProgressBar, ProgressDisplay}
import ccas.utils.errors.safeMessage
import ccas.utils.json.JsonDecodingException

/** HTTP client for the Chess.com public API with adaptive rate limiting.
  *
  * Wraps a `zio-http` `Client` and adds several layers of concurrency and error management:
  *
  *   - '''Gate-based admission control''' — a single-permit gate serializes request admission. Before each request, the
  *     gate checks that the number of in-flight requests (`activeRef`) is below `currentMax`. If not, it polls until a
  *     slot opens, then atomically increments the slot count before releasing the gate. This enforces the concurrency
  *     limit without a semaphore, so throttle-down takes effect immediately for all requests that haven't yet entered
  *     the gate. The gate wait is interruptible so that pending requests can be cancelled promptly on shutdown.
  *   - '''EMA-based rate delay''' — tracks an exponential moving average of response times and staggers outgoing
  *     requests so that the full permit budget is utilised without bursting. When permits are reduced the per-request
  *     delay grows proportionally. A configurable floor (`min-request-delay-ms`) prevents burst behaviour when
  *     response times are unusually low.
  *   - '''Failure-window throttle-down''' — maintains a rolling window of success/failure outcomes. HTTP 429 counts as a
  *     failure; non-rate-limit responses (403 non-CF, 404, 500, etc.) count as successes. Cloudflare challenge 403s
  *     bypass the window and trigger an immediate hard throttle. Connection errors are retried but do not feed the
  *     window (reducing permits doesn't fix a broken network). When the failure rate in the window exceeds a
  *     configurable threshold, `currentMax` drops to 1 and the gate immediately enforces it.
  *   - '''Generation-gated recovery''' — after a cooldown period, a background fiber walks `currentMax` up through the
  *     configured `recoveryTiers` (one step per cooldown cycle), or holds if failures persist. Each tier must be
  *     observed for at least `min-tier-observation-seconds` before being evaluated for promotion, preventing premature
  *     step-ups when high concurrency fills the outcome window quickly. `coolingDown` stays true throughout the
  *     recovery ladder to prevent `recordOutcome` from triggering additional throttle-downs mid-recovery; it is only
  *     cleared when permits reach `maxPermits`. A generation counter ensures that only the most recent throttle-down
  *     triggers recovery, preventing stale fibers from interfering. The response-time EMA is reset to zero on full
  *     recovery so that stale inflated values do not gate post-recovery request pacing. Recovery fibers are scoped to
  *     the client's lifetime and interrupted when the layer is torn down.
  *   - '''Retry schedules''' — separate schedules handle HTTP 429 (exponential backoff), Cloudflare challenge 403s
  *     (fixed delay), and transient connection errors (exponential backoff); each has an independent retry-count
  *     budget configured via `max-429-retries`, `max-cf-retries`, and `max-connection-retries`. Non-Cloudflare 403
  *     and HTTP 404 are treated as permanent and never retried.
  *   - '''Cumulative stats flushing''' — a background daemon fiber upserts a single `client_stats` row per session
  *     every `stats-flush-interval-seconds`, overwriting the previous snapshot with cumulative totals. The throttle
  *     configuration is inserted once into `client_config` on first flush and referenced by FK. A final flush runs
  *     in the scope finalizer to ensure stats survive non-graceful shutdowns with at most one interval's worth of
  *     data loss.
  *   - '''Response caching''' — every successful fetch is persisted to `api_response_cache`, keyed by URL, with the
  *     ETag / Last-Modified / `Cache-Control: max-age` / Content-Type metadata and a FK into `api_response_body`
  *     (whose SHA-256 dedupe means byte-identical bodies share one row even across URLs). The public [[getCacheable]]
  *     entry point short-circuits to a `Fresh` result when the cache entry is still within `max-age`, never entering
  *     the gate or sending a request. Stale entries are sent as conditional GETs (`If-None-Match` +
  *     `If-Modified-Since`); a 304 response returns `Revalidated` and bumps `fetched_at` without re-downloading.
  *     A 200 that dedupes to the same `body_id` returns `IdenticalBody`; otherwise `Changed`. `Cache-Control:
  *     no-store` is honoured (response is not cached). Cache hits and revalidations are tracked as separate counters
  *     on `ClientStatsAccumulator` so the `requests` / `successes` / `failures` numbers continue to reflect only real
  *     Chess.com API load.
  *
  * Constructed via the `ChessComClient.live` ZLayer which reads configuration from `application.conf` under the
  * `chess-com-client` prefix.
  */
final class ChessComClient(
  client: Client,
  pgClient: PostgresClient,
  bodyStore: BodyStore,
  headers: Headers,
  throttle: ChessComClient.ThrottleRefs,
  statsRef: Ref[ClientStatsAccumulator],
  progressBar: ProgressBar,
  config: ChessComClient.ThrottleConfig,
  recoveryFiberRef: Ref[Option[Fiber.Runtime[Throwable, Unit]]],
  endSessionEffect: UIO[Unit]
) {
  import throttle.*

  /** Top of the gate's recovery ladder — the maximum number of in-flight requests this instance will permit at
    * steady state. Exposed for callers that size their fiber pools relative to the gate (e.g. `ApiConcurrency`).
    */
  def maxPermits: Int = config.maxPermits.toInt

  /** Pins the client-stats session window to the current instant and stops the periodic flush fiber. Call once the
    * last API request has completed but before any human pause (e.g. a CLI confirmation prompt) so the persisted
    * session window and throughput reflect API work, not wall-clock-until-exit. Idempotent and safe to skip.
    */
  def endSession: UIO[Unit] = endSessionEffect

  // `ZClientAspect.followRedirects` treats every 3xx as a redirect, including 304 Not Modified, and fails on the
  // missing Location header. Return the 304 response as-is so conditional-GET revalidation works; other redirect
  // failures still propagate as errors.
  private val batchedClient = client.batched @@ ZClientAspect.followRedirects(3) { (resp, message) =>
    if (resp.status == Status.NotModified) ZIO.succeed(resp)
    else ZIO.fail(Exception(s"Redirect failed: $message"))
  }

  private def rawGet[T](url: URL, conditional: Option[ApiResponseCache], cacheWrites: Boolean)(
    using jsonDecoder: JsonDecoder[T]
  ): Task[CacheableResult[T]] = {
    val request = buildRequest(url, conditional)
    (for {
      tier <- stateRef.get.map(_.currentMax.toInt)
      _    <- statsRef.update(_.incAttemptAtTier(tier))
      response <- batchedClient(request).tapError { e =>
        ZIO.whenDiscard(ConnectionError.isConnectionError(e))(statsRef.update(_.incConnectionErrors))
      }
      result <- handleResponse[T](url, conditional, response, tier, cacheWrites)
    } yield result).tapError { e =>
      val (errorType, msg, body) = e match {
        case e: HttpStatusException => (e.getClass.getSimpleName, Some(e.statusCode.toString), Some(e.responseBody))
        case other                  => (other.getClass.getSimpleName, Option(other.getMessage), None)
      }
      ApiFetchFailure
        .insert(ApiFetchFailure(Instant.now(), url.encode, errorType, msg, body))
        .provideEnvironment(ZEnvironment(pgClient, bodyStore))
        .tapError(dbErr => ZIO.logWarning(s"Failed to record api_fetch_failure for ${url.encode}: ${dbErr.safeMessage}"))
        .ignore
    }
  }

  /** Build the outgoing request, attaching `If-None-Match` and/or `If-Modified-Since` validators when a prior cache
    * entry is present. Both headers are sent when available for standards correctness, but empirically the current
    * Chess.com API honours only `If-None-Match` — `If-Modified-Since` is silently ignored regardless of value
    * (verified 2026-04-17; Chess.com have confirmed the current API is in maintenance mode, with a future rewrite
    * that may or may not change this). The echo path stays wired so we're ready if the next API starts honouring it.
    *
    * Note: `If-None-Match` is attached via `Header.Custom` with the raw wire-format etag (quotes included). zio-http
    * 3.10.1's typed `Header.IfNoneMatch.ETags` strips quotes during render (see `IfNoneMatch.render` in zio-http
    * sources), which is RFC 7232–incorrect and produces a header the origin won't match. The stored etag in
    * `api_response_cache.etag` is already in wire format — `extractValidators` reads via the typed `Header.ETag`
    * and re-renders through `Header.ETag.render` on the persist path — so it can be echoed back verbatim here.
    */
  private def buildRequest(url: URL, conditional: Option[ApiResponseCache]): Request = {
    val base = Request(method = GET, url = url).addHeaders(headers)
    conditional match {
      case None => base
      case Some(meta) =>
        val withEtag = meta.etag.fold(base)(et => base.addHeader(Header.Custom("If-None-Match", et)))
        meta.lastModified.fold(withEtag) { lm =>
          withEtag.addHeader(Header.IfModifiedSince(lm.atZone(ZoneOffset.UTC)))
        }
    }
  }

  /** Dispatch on response status. 304 is handled before the generic error path so conditional revalidation bypasses
    * `HttpStatusException`. 2xx is routed to `handleSuccessBody` for cache upsert and decode. Everything else flows
    * through the existing error-recording machinery unchanged.
    */
  private def handleResponse[T](
    url: URL,
    conditional: Option[ApiResponseCache],
    response: Response,
    tier: Int,
    cacheWrites: Boolean
  )(using jsonDecoder: JsonDecoder[T]): Task[CacheableResult[T]] = {
    if (response.status == Status.NotModified) {
      conditional match {
        case Some(meta) => handleNotModified[T](url, meta, response, cacheWrites)
        case None       => ZIO.fail(Exception(s"Unexpected 304 Not Modified for non-conditional request: $url"))
      }
    } else {
      response.body.asString.flatMap { string =>
        val cfChallenge = isCloudflareChallenge(response, string)
        val errorBody   = if (cfChallenge) ApiResponseBody.CfCanonicalBody else string
        val outcomeEffect =
          if (cfChallenge) throttleDown(config.cfCooldown)
          else recordOutcome(response.status != Status.TooManyRequests)
        val errorPath =
          if (response.status.isSuccess) {
            handleSuccessBody[T](url, conditional, response, string, cacheWrites)
          } else {
            val errorUpdate =
              if (cfChallenge) statsRef.update(_.incCf403AtTier(tier))
              else if (response.status.code == 429) statsRef.update(_.incError429AtTier(tier))
              else statsRef.update(_.incErrorOther)
            errorUpdate *> ZIO.fail(HttpStatusException.classify(response.status.code, url, errorBody))
          }
        outcomeEffect *> errorPath
      }
    }
  }

  /** Read the cache-relevant validators from a response in a single place so the 200 and 304 paths can't drift
    * apart. Notably, `Last-Modified` must go through [[HttpDate.parse]] because Chess.com's wire format is not
    * RFC 7231 HTTP-date (see [[HttpDate]]); ETag goes through the typed parser and is re-rendered so the stored
    * value is in wire format (`"..."` / `W/"..."`), matching what we echo back via `Header.Custom("If-None-Match", …)`.
    */
  private def extractValidators(response: Response): ChessComClient.ResponseValidators =
    ChessComClient.ResponseValidators(
      etag = response.header(Header.ETag).map(Header.ETag.render),
      lastModified = response.rawHeader("Last-Modified").flatMap(HttpDate.parse),
      contentType = response.header(Header.ContentType).map(_.mediaType.fullType)
    )

  // If the origin sends an ETag we couldn't parse, the next request goes out without `If-None-Match`
  // and a 200 comes back instead of a 304. Surfacing a debug log makes the regression visible.
  private def logEtagParseMiss(response: Response): zio.UIO[Unit] = {
    val raw   = response.rawHeader("ETag")
    val typed = response.header(Header.ETag)
    ZIO.whenDiscard(raw.isDefined && typed.isEmpty)(
      ZIO.logDebug(s"ETag header present but unparseable by zio-http: ${raw.getOrElse("")}")
    )
  }

  /** 304 path: bump `fetched_at` and merge any fresh validators or cache-control value from the response. 304s
    * count as a success for the failure window (the origin is reachable and willing to serve us) — the `true`
    * argument to `recordOutcome` keeps the non-429 branch, same as any other non-rate-limit response.
    *
    * `maxAgeUpdate` carries the wire-level intent — `Preserve` when no `Cache-Control` header is present,
    * `Clear` for `no-cache` (matching the 200 path at [[handleSuccessBody]]), and `Overwrite(n)` for
    * `max-age=n`. ETag / Last-Modified / Content-Type use COALESCE semantics inside [[ApiResponseCache.touch]],
    * so a 304 that omits any of them preserves the stored value.
    */
  private def handleNotModified[T](
    url: URL,
    meta: ApiResponseCache,
    response: Response,
    cacheWrites: Boolean
  )(using jsonDecoder: JsonDecoder[T]): Task[CacheableResult[T]] = {
    val directives = parseCacheDirectives(response)
    val maxAgeUpdate: ApiResponseCache.MaxAgeUpdate =
      if (response.header(Header.CacheControl).isEmpty) ApiResponseCache.MaxAgeUpdate.Preserve
      else if (directives.noCache) ApiResponseCache.MaxAgeUpdate.Clear
      else directives.maxAgeSeconds.fold[ApiResponseCache.MaxAgeUpdate](ApiResponseCache.MaxAgeUpdate.Clear)(
        ApiResponseCache.MaxAgeUpdate.Overwrite(_)
      )
    val validators = extractValidators(response)
    val touchEffect =
      ZIO.whenDiscard(cacheWrites) {
        ApiResponseCache
          .touch(url.encode, Instant.now(), validators.etag, validators.lastModified, maxAgeUpdate, validators.contentType)
          .provideEnvironment(ZEnvironment(pgClient))
          .unit
      }
    // `incCacheRevalidation` fires before `touchEffect` on purpose: the stat reports Chess.com API load (a 304 was
    // received), not DB-write success. A failed `touch` shouldn't blank the load attribution. Asymmetric with
    // `handleSuccessBody`'s post-upsert bumps, which need the upsert's `body_id` for hit-vs-miss discrimination.
    for {
      _ <- logEtagParseMiss(response)
      _ <- recordOutcome(true)
      _ <- statsRef.update(_.incCacheRevalidation)
      _ <- touchEffect
    } yield CacheableResult.Revalidated(meta.bodyId, loadAndDecode[T](url, meta.bodyId, cacheWrites))
  }

  /** Success path: extract cache-control headers, upsert the response body into the cache (unless `no-store`), and
    * return `IdenticalBody` when the new body deduped to the same `body_id` as the prior cache entry, otherwise
    * `Changed` with the eagerly-decoded value. A body-store outage makes the upsert a no-op (see
    * [[ApiResponseCache.upsertWithBody]]) and the result `Changed` — the value is already in memory, so the request
    * succeeds uncached rather than failing on a non-authoritative cache.
    */
  private def handleSuccessBody[T](
    url: URL,
    conditional: Option[ApiResponseCache],
    response: Response,
    string: String,
    cacheWrites: Boolean
  )(using jsonDecoder: JsonDecoder[T]): Task[CacheableResult[T]] = {
    val directives = parseCacheDirectives(response)
    val validators = extractValidators(response)
    // RFC 7234 §5.2.2.2: `Cache-Control: no-cache` means "cache but always revalidate before reuse". We honour it
    // by dropping any `max-age` so `isFresh` never returns true — subsequent requests go out as conditional GETs
    // (validated via etag / last-modified) rather than being served locally.
    val effectiveMaxAge = if (directives.noCache) None else directives.maxAgeSeconds
    // `newBodyIdOpt` is `None` in three cases: the caller opted out of cache writes, the response said `no-store`,
    // or the body store rejected the write (outage — `upsertWithBody` then persists nothing at all). All three land
    // on the `Changed` arm below and increment `cacheMisses`, never `IdenticalBody`. That is intentional: hit-vs-miss
    // discrimination requires comparing the new body's `body_id` against the prior one, which only the upsert
    // produces. Don't try to "fix" by SHA-256-ing the body to reconstruct the comparison — the stat is meant to
    // count Chess.com bytes downloaded, and an uncached caller has by definition just downloaded fresh bytes.
    val upsertEffect: Task[Option[ApiResponseBodyId]] =
      if (!cacheWrites || directives.noStore) ZIO.none
      else
        ApiResponseCache
          .upsertWithBody(
            url = url.encode,
            body = string,
            etag = validators.etag,
            lastModified = validators.lastModified,
            maxAgeSeconds = effectiveMaxAge,
            contentType = validators.contentType,
            fetchedAt = Instant.now()
          )
          .provideEnvironment(ZEnvironment(pgClient, bodyStore))
    val decodeLazy = ZIO.fromEither(jsonDecoder.decodeJson(string)).mapError(JsonDecodingException(_))
    for {
      _            <- logEtagParseMiss(response)
      newBodyIdOpt <- upsertEffect
      result <- (newBodyIdOpt, conditional.map(_.bodyId)) match {
        case (Some(newBodyId), Some(oldBodyId)) if newBodyId == oldBodyId =>
          statsRef.update(_.incCacheHit).as(CacheableResult.IdenticalBody(newBodyId, decodeLazy))
        case _ =>
          statsRef.update(_.incCacheMiss) *> decodeLazy.map(CacheableResult.Changed(_))
      }
    } yield result
  }

  private def parseCacheDirectives(response: Response): ChessComClient.CacheDirectives = {
    def walk(cc: Header.CacheControl, acc: ChessComClient.CacheDirectives): ChessComClient.CacheDirectives = cc match {
      case Header.CacheControl.MaxAge(n)     => acc.copy(maxAgeSeconds = Some(n.toLong))
      case Header.CacheControl.NoStore       => acc.copy(noStore = true)
      case Header.CacheControl.NoCache       => acc.copy(noCache = true)
      case Header.CacheControl.Multiple(nec) => nec.foldLeft(acc)((a, d) => walk(d, a))
      case _                                 => acc
    }
    response
      .header(Header.CacheControl)
      .fold(ChessComClient.CacheDirectives.empty)(walk(_, ChessComClient.CacheDirectives.empty))
  }

  /** Lazy body-load + decode for `Fresh` and `Revalidated` results. Every recovery path here refetches
    * unconditionally — without a body we cannot serve a 304, so sending validators would risk coming back with
    * metadata and still nothing to serve. The recovery inherits the original caller's `cacheWrites` flag so an
    * uncached caller doesn't silently re-cache via the recovery path, and is bounded to a single attempt:
    * `catchSome` is scoped to the cached-body decode only, so a fresh-body decode failure (persistent origin-side
    * bug) propagates instead of looping back through `catchSome → refetch → catchSome`.
    *
    * What differs across the three failures is whether the '''cache row survives''' (#215):
    *
    *   - Decode failure (schema drift: the body is there but no longer parses) and [[BodyRead.Missing]] (pruned
    *     pointer row, or an object absent from the store) both mean the row points at something unusable, so it is
    *     dropped.
    *   - [[BodyRead.Unavailable]] — the store errored or outran its deadline — means the row is still accurate. It
    *     is '''kept'''. Dropping it would spend a transient outage's worth of validators permanently: every URL
    *     touched while the store was down would lose its ETag and `max-age` and pay a full unconditional GET after
    *     recovery instead of a cheap 304. The refetch's own `upsertWithBody` no-ops while the store is still down
    *     (`putOrSkip` returns false), leaving the row intact to self-heal on the first successful put.
    *
    * Every path also records `cacheUnserved`, the reconciling term for the optimistic `cacheHit` / `cacheRevalidation`
    * counted before the body was ever read — see [[ClientStatsAccumulator.cacheUnserved]].
    */
  private def loadAndDecode[T](url: URL, bodyId: ApiResponseBodyId, cacheWrites: Boolean)(
    using jsonDecoder: JsonDecoder[T]
  ): Task[T] = {
    // No `lookupMeta` round-trip on the way back in: the row was either just deleted or is deliberately being
    // preserved, and in both cases the next step is the same unconditional GET the miss arm of `getCacheableImpl`
    // would have dispatched.
    val refetch =
      statsRef.update(_.incCacheUnserved.incRequests) *>
        withRetries(gatedRawGet[T](url, conditional = None, cacheWrites)).flatMap(_.getValue)
    val invalidateAndRefetch = ApiResponseCache
      .invalidate(url.encode)
      .provideEnvironment(ZEnvironment(pgClient))
      .ignore *> refetch
    ApiResponseBody
      .loadById(bodyId)
      .provideEnvironment(ZEnvironment(pgClient, bodyStore))
      .flatMap {
        case BodyRead.Found(body) =>
          ZIO.fromEither(jsonDecoder.decodeJson(body))
            .mapError(JsonDecodingException(_))
            .catchSome { case _: JsonDecodingException => invalidateAndRefetch }
        // The BodyStore is content-addressed and so URL-agnostic; its own logs can only name a hash. This is the
        // nearest frame that knows which endpoint lost its cached body, so it is where the URL gets recorded.
        case BodyRead.Missing =>
          ZIO.logDebug(s"Cached body missing for ${url.encode}; invalidating and refetching") *> invalidateAndRefetch
        case BodyRead.Unavailable =>
          ZIO.logDebug(
            s"Body store could not serve the cached body for ${url.encode}; refetching and keeping the cache entry"
          ) *> refetch
      }
  }

  /** True if a cached entry is still within its `Cache-Control: max-age` window. Entries without a `max-age` are
    * never fresh — we always revalidate (via conditional headers, or a full fetch if no validators are available).
    */
  private def isFresh(meta: ApiResponseCache, now: Instant): Boolean =
    meta.maxAgeSeconds.exists(maxAge => now.isBefore(meta.fetchedAt.plusSeconds(maxAge)))

  /** Wrap the existing gate / permit / latency-timing block around `rawGet`, parameterised by the optional
    * conditional cache entry. All throttle, retry, and error-recording machinery lives inside here.
    */
  private def gatedRawGet[T](url: URL, conditional: Option[ApiResponseCache], cacheWrites: Boolean)(
    using jsonDecoder: JsonDecoder[T]
  ): Task[CacheableResult[T]] =
    ZIO.scoped {
      for {
        _ <- rateLimitGate.withPermit {
          for {
            (gateWait, _) <- awaitCapacity.timed
            (emaWait, _)  <- emaDelay.timed
            active        <- ZIO.acquireRelease(activeRef.updateAndGet(_ + 1).tap(updateBar))(_ => releaseSlot())
            _ <- statsRef.update(_.addGateWait(gateWait.toMillis).addEmaDelay(emaWait.toMillis).updatePeak(active))
          } yield ()
        }
        (duration, result) <- rawGet(url, conditional, cacheWrites).timed
        ms = duration.toMillis
        _ <- updateResponseTimeEma(ms)
        _ <- statsRef.update(_.recordLatency(ms).addActiveMs(ms))
      } yield result
    }

  /** Cache-aware entry point. Checks `api_response_cache` first; on a fresh hit (within `max-age`) returns a
    * `Fresh` result without a network call. Otherwise dispatches to the gated + retried `rawGet`, passing any prior
    * cache row so `If-None-Match` / `If-Modified-Since` validators can be attached. Callers that want to skip
    * downstream processing on unchanged data should use this directly; callers that just want `T` should use `get`,
    * or `getUncached` to suppress cache writes for one-shot volatile-body endpoints.
    *
    * Error contract: a connection / DNS error that survives the connection-retry schedule (the network was
    * unreachable across all attempts, not a one-off blip) surfaces as [[NetworkUnavailableException]] (with the
    * underlying transport exception as its cause). Match that type to react to a systemic outage; HTTP errors still
    * surface as [[HttpStatusException]] / [[ReportedNotFound]].
    */
  def getCacheable[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[CacheableResult[T]] =
    getCacheableImpl[T](url, cacheWrites = true)

  /** Shared implementation behind `getCacheable` (cacheWrites = true) and `getUncached` (cacheWrites = false). When
    * `cacheWrites` is false, a successful 200 response is decoded but never written to `api_response_cache` /
    * `api_response_body`, and a 304 does not bump `fetched_at` on any pre-existing cache row. The read path is
    * unchanged: a `Fresh` row from another caller is still served without a network call.
    */
  private def getCacheableImpl[T](url: URL, cacheWrites: Boolean)(
    using jsonDecoder: JsonDecoder[T]
  ): Task[CacheableResult[T]] =
    ApiResponseCache
      .lookupMeta(url.encode)
      .provideEnvironment(ZEnvironment(pgClient))
      .flatMap {
        case Some(meta) if isFresh(meta, Instant.now()) =>
          statsRef
            .update(_.incCacheHit)
            .as(CacheableResult.Fresh(meta.bodyId, loadAndDecode[T](url, meta.bodyId, cacheWrites)))
        case cachedOpt =>
          statsRef.update(_.incRequests) *> withRetries(gatedRawGet[T](url, cachedOpt, cacheWrites))
      }

  def get[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] =
    getCacheable[T](url).flatMap(_.getValue)

  /** Like `get` but skips cache writes (and 304 `touch`). Reads still hit the cache. */
  def getUncached[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] =
    getCacheableImpl[T](url, cacheWrites = false).flatMap(_.getValue)

  private def withRetries[T](effect: Task[T]): Task[T] =
    effect
      .retry(retry429Schedule)
      .retry(retryCfSchedule)
      .retry(retryConnectionSchedule)
      .mapError {
        // The connection-retry schedule is exhausted by the time we get here: a connection error that survives means
        // the network has been unreachable across all attempts. Surface a single typed error so each caller can decide
        // what's appropriate (abort, flush partial data, log, ignore). The wrap happens outside `rawGet`'s `tapError`,
        // so `api_fetch_failure` still records the real underlying type per attempt; `isConnectionError` returns false
        // for an already-wrapped error, so recursive refetch paths never double-wrap.
        case e if ConnectionError.isConnectionError(e) => new NetworkUnavailableException(e)
        case e                                         => e
      }
      .tapBoth(
        _ => statsRef.update(_.incFailures),
        _ => statsRef.update(_.incSuccesses)
      )

  /** Parallel fetch across many URLs, capped at `maxPermits` in-flight fibers. Network-bound fibers are already
    * serialised to `maxPermits` by the rate-limit gate inside `gatedRawGet`, so capping the outer `foreachPar` at
    * the same number is free for that path — extra fibers would just queue at the gate. For cache-warm workloads
    * (where fibers never enter the gate), the cap bounds concurrent `lookupMeta` / `loadById` against the Hikari
    * connection pool at a predictable level. `lookupMeta` is a sub-millisecond indexed point-lookup, so even at
    * `maxPermits` parallelism throughput is high enough that the cap is not the bottleneck in practice.
    */
  def getAll[T](urls: Iterable[URL])(using jsonDecoder: JsonDecoder[T]): Task[Chunk[T]] =
    ZIO.foreachPar(Chunk.from(urls))(get).withParallelism(config.maxPermits.toInt)

  // ---------------------------------------------------------------------------
  // Progress bar
  // ---------------------------------------------------------------------------

  private def updateBar(active: Int): ZIO[Any, Nothing, Unit] =
    stateRef.get.flatMap { state =>
      val suffix = if (state.currentMax < config.maxPermits) " (throttled)" else ""
      progressBar.print(active, state.currentMax.toInt, s"  API: $active/${state.currentMax.toInt}$suffix")
    }

  // ---------------------------------------------------------------------------
  // Adaptive throttle
  // ---------------------------------------------------------------------------

  private def releaseSlot(): UIO[Unit] =
    activeRef.updateAndGet(_ - 1).flatMap(updateBar).ignore

  /** Poll until an active slot is available. Runs inside the serializing admission gate; the slot increment happens
    * inside the same gate-protected region (via `ZIO.acquireRelease` in `get`) so that the capacity check and
    * increment are atomic with respect to other admitting fibers. The polling itself is interruptible so that
    * pending requests can be cancelled promptly on shutdown.
    */
  private def awaitCapacity: Task[Unit] =
    (for {
      active <- activeRef.get
      state  <- stateRef.get
    } yield active < state.currentMax.toInt)
      .repeat(Schedule.spaced(10.millis) && Schedule.recurWhile(!_)).unit

  private def emaDelay: Task[Unit] =
    ZIO.whenDiscard(config.maxPermits > 1) {
      stateRef.get.flatMap { state =>
        ZIO.whenDiscard(state.responseTimeEma > 0) {
          val targetDelay = math.max((state.responseTimeEma / state.currentMax).toLong, config.minRequestDelayMs)
          for {
            now <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
            last <- lastRequestRef.get
            gap = now - last
            _ <- ZIO.whenDiscard(gap < targetDelay)(ZIO.sleep((targetDelay - gap).millis))
            _ <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap(lastRequestRef.set)
          } yield ()
        }
      }
    }

  /** Wall-clock EMA update. `alpha = 1 - exp(-dt / emaTauMs)` so an outlier sample's influence decays in real time:
    * a slow response that holds the fiber for tens of seconds is naturally cancelled by the very next sample's large
    * `dt`. `dt` is intentionally NOT capped — after a long idle the prior EMA is stale, and snapping to the new sample
    * (alpha → 1) is the correct behaviour. The first sample (or first after `responseTimeEma` was reset on full
    * recovery) seeds the EMA directly.
    */
  private def updateResponseTimeEma(responseMs: Long): UIO[Unit] =
    Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap { now =>
      stateRef.update { state =>
        val newEma =
          if (state.responseTimeEma <= 0 || state.lastEmaSampleAt == 0L) {
            responseMs.toDouble
          } else {
            // Floor dt at 1 ms so two concurrent updates landing on the same wall-clock millisecond
            // still contribute a (tiny) alpha rather than being silently dropped via alpha = 0.
            val dt    = (now - state.lastEmaSampleAt).max(1L)
            val alpha = 1.0 - math.exp(-dt.toDouble / config.emaTauMs.toDouble)
            alpha * responseMs + (1 - alpha) * state.responseTimeEma
          }
        state.copy(responseTimeEma = newEma, lastEmaSampleAt = now)
      }
    }

  /** Record a rate-limit outcome (429 vs non-429) and trigger throttle-down if failure rate exceeds threshold. Only
    * called for HTTP responses, not connection errors — transient network failures are retried but do not feed the
    * failure window. Skipped while cooling down to prevent cascading reductions from a single burst of failures.
    */
  private def recordOutcome(success: Boolean): Task[Unit] =
    stateRef.modify { state =>
      val newOutcomes =
        if (state.outcomes.size >= config.failureWindowSize) { state.outcomes.tail :+ success }
        else { state.outcomes :+ success }
      val shouldThrottle = !success &&
        !state.coolingDown &&
        newOutcomes.size >= config.minSampleSize &&
        state.currentMax > 1 &&
        failureRate(newOutcomes) > config.failureThreshold
      (shouldThrottle, state.copy(outcomes = newOutcomes))
    }.flatMap(shouldThrottle => ZIO.whenDiscard(shouldThrottle)(throttleDown()))

  /** Drop the effective concurrency limit to 1 and schedule drain-then-recover. Clears the outcome window and sets
    * coolingDown to prevent cascading reductions. The `cooldown` parameter controls how long to wait after in-flight
    * requests have drained before attempting recovery.
    */
  private def throttleDown(cooldown: Duration = config.cooldown): Task[Unit] =
    for {
      now <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
      transitionOpt <- stateRef.modify { state =>
        if (state.currentMax <= 1) {
          (None, state)
        } else {
          val newGen = state.generation + 1
          val newState = state.copy(
            currentMax = 1,
            generation = newGen,
            outcomes = Vector.empty,
            coolingDown = true,
            throttledSince = Some(now),
            tierEnteredAt = Some(now)
          )
          (Some((state.currentMax, newGen)), newState)
        }
      }
      _ <- ZIO.foreachDiscard(transitionOpt) { case (oldMax, gen) => applyThrottle(oldMax, gen, cooldown) }
    } yield ()

  private def applyThrottle(oldMax: Long, gen: Long, cooldown: Duration): Task[Unit] =
    for {
      _     <- statsRef.update(_.incThrottleDowns)
      _     <- ProgressDisplay.sourced("rate-limit")(ZIO.logWarning(s"Rate limit throttle: $oldMax \u2192 1 permit"))
      fiber <- scheduleRecovery(gen, cooldown).forkDaemon
      // Interrupt-and-replace so at most one recovery fiber is ever retained: a fresh throttle-down supersedes any
      // in-flight recovery (the older one is already a generation-stale no-op — see the `generation` guard in
      // `scheduleRecovery`). A single scope finalizer (in `live`) interrupts whatever fiber remains at shutdown.
      // `interruptFork` so this response-path effect never blocks awaiting the superseded fiber's teardown.
      prev  <- recoveryFiberRef.getAndSet(Some(fiber))
      _     <- ZIO.foreachDiscard(prev)(_.interruptFork)
    } yield ()

  /** Wait for in-flight requests to drain, sleep for cooldown, then recover permits if failure rate has dropped. After
    * the cooldown, enforces `minTierObservation` — if not enough wall-clock time has elapsed since the current tier was
    * entered, the fiber sleeps the remainder so that each tier is observed under real load before being evaluated. If
    * the failure rate is still above threshold, drops back one tier (via `previousTier`) to find a sustainable level
    * rather than holding at a tier that's too aggressive. Clears the outcome window and resets `tierEnteredAt` on each
    * step so every tier is evaluated on its own merits. Keeps coolingDown true throughout the recovery ladder to prevent
    * mid-recovery re-throttling; clears it only when permits reach maxPermits. Resets the response-time EMA on full
    * recovery.
    */
  private def scheduleRecovery(generation: Long, cooldown: Duration): Task[Unit] =
    for {
      _ <- activeRef.get.repeat(Schedule.spaced(200.millis) && Schedule.recurWhile(_ > 1)).unit
      _ <- ZIO.sleep(cooldown)
      _ <- stateRef.get.flatMap { state =>
        state.tierEnteredAt match {
          case Some(enteredAt) =>
            Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap { now =>
              val remaining = config.minTierObservation.toMillis - (now - enteredAt)
              ZIO.whenDiscard(remaining > 0)(ZIO.sleep(remaining.millis))
            }
          case None => ZIO.unit
        }
      }
      now <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
      option <- stateRef.modify { state =>
        if (state.generation != generation) (None, state)
        else if (failureRate(state.outcomes) > config.failureThreshold) {
          val dropTo = config.previousTier(state.currentMax)
          val newState = state.copy(currentMax = dropTo, outcomes = Vector.empty, tierEnteredAt = Some(now))
          (Some((state.currentMax, dropTo, generation, 0L)), newState)
        } else {
          val newMax         = config.nextTier(state.currentMax)
          val fullyRecovered = newMax == config.maxPermits
          val throttleDuration =
            if (fullyRecovered) state.throttledSince.fold(0L)(now - _)
            else 0L
          val newState = state.copy(
            currentMax = newMax,
            coolingDown = !fullyRecovered,
            outcomes = Vector.empty,
            throttledSince = if (fullyRecovered) None else state.throttledSince,
            tierEnteredAt = if (fullyRecovered) None else Some(now),
            responseTimeEma = if (fullyRecovered) 0.0 else state.responseTimeEma,
            lastEmaSampleAt = if (fullyRecovered) 0L else state.lastEmaSampleAt
          )
          (Some((state.currentMax, newMax, generation, throttleDuration)), newState)
        }
      }
      _ <- ZIO.foreachDiscard(option) { case (oldMax, newMax, gen, throttleDuration) =>
        ZIO.whenDiscard(throttleDuration > 0)(statsRef.update(_.addThrottled(throttleDuration))) *> {
            if (newMax == oldMax) {
              scheduleRecovery(gen, cooldown)
            } else if (newMax < oldMax) {
              ProgressDisplay.sourced("rate-limit")(
                ZIO.logWarning(s"Rate limit dropping back: $oldMax \u2192 $newMax permit(s)")
              ) *>
                scheduleRecovery(gen, cooldown)
            } else {
              val msg =
                if (newMax == config.maxPermits) "Rate limit throttle lifted"
                else s"Rate limit easing: $oldMax \u2192 $newMax permits"
              ProgressDisplay.sourced("rate-limit")(ZIO.logInfo(msg)) *>
                ZIO.unlessDiscard(newMax == config.maxPermits)(scheduleRecovery(gen, cooldown))
            }
          }
      }
    } yield ()

  private val CfChallengeMarker = "/cdn-cgi/challenge-platform/"

  private def isCloudflareChallenge(response: Response, body: String): Boolean =
    response.status == Status.Forbidden && body.contains(CfChallengeMarker)

  private def failureRate(outcomes: Vector[Boolean]): Double =
    if (outcomes.isEmpty) 0.0 else outcomes.count(!_).toDouble / outcomes.size

  private val retry429Schedule: Schedule[Any, Throwable, Any] =
    Schedule.exponential(config.retryBase).jittered && Schedule.recurs(config.max429Retries) && Schedule.recurWhile[Throwable] {
      case e: HttpStatusException => e.statusCode == 429
      case _                      => false
    }

  private val retryCfSchedule: Schedule[Any, Throwable, Any] =
    Schedule.fixed(config.cfRetryDelay) && Schedule.recurs(config.maxCfRetries) && Schedule.recurWhile[Throwable] {
      case e: HttpStatusException => e.statusCode == 403 && e.responseBody.contains(CfChallengeMarker)
      case _                      => false
    }

  private val retryConnectionSchedule: Schedule[Any, Throwable, Any] =
    Schedule.exponential(config.connectionRetryBase) && Schedule.recurs(config.maxConnectionRetries) &&
      Schedule.recurWhile[Throwable](ConnectionError.isConnectionError)
}

object ChessComClient {

  /** Parsed subset of `Cache-Control` directives that influence our caching decisions. Anything outside these three
    * (e.g. `must-revalidate`, `immutable`, `public`, `private`) is ignored.
    */
  private[ccas] final case class CacheDirectives(
    maxAgeSeconds: Option[Long],
    noStore: Boolean,
    noCache: Boolean
  )

  private[ccas] object CacheDirectives {
    val empty: CacheDirectives = CacheDirectives(None, false, false)
  }

  /** Validators and content-type extracted from a response and persisted alongside the cached body. Shared by the
    * 200 (`handleSuccessBody`) and 304 (`handleNotModified`) paths so the header-parsing rules live in one place.
    */
  private[ccas] final case class ResponseValidators(
    etag: Option[String],
    lastModified: Option[Instant],
    contentType: Option[String]
  )

  private[ccas] case class ThrottleRefs(
    stateRef: Ref[ThrottleState],
    activeRef: Ref[Int],
    rateLimitGate: Semaphore,
    lastRequestRef: Ref[Long]
  )

  private[ccas] case class ThrottleState(
    currentMax: Long,
    generation: Long,
    outcomes: Vector[Boolean],
    coolingDown: Boolean = false,
    throttledSince: Option[Long] = None,
    tierEnteredAt: Option[Long] = None,
    responseTimeEma: Double = 0.0,
    lastEmaSampleAt: Long = 0L
  )

  /** @param recoveryTiers
    *   Strictly-increasing permit counts (all >= 2) that define the recovery ladder. Throttle-down always drops to 1;
    *   recovery walks up the tiers until reaching the last value, which is the maximum permit count (`maxPermits`).
    * @param cooldown
    *   Time to wait after draining in-flight requests before attempting recovery from a failure-rate throttle-down.
    * @param cfCooldown
    *   Time to wait after draining in-flight requests before attempting recovery from a Cloudflare throttle-down.
    * @param retryBase
    *   Base duration for exponential-backoff retry on 429 responses.
    * @param cfRetryDelay
    *   Fixed delay between retries on Cloudflare challenge 403 responses.
    * @param connectionRetryBase
    *   Base duration for exponential-backoff retry on transient connection errors.
    * @param max429Retries
    *   Maximum number of retries on 429 responses (0 = no retries, i.e. fail on first 429).
    * @param maxCfRetries
    *   Maximum number of retries on Cloudflare challenge 403 responses.
    * @param maxConnectionRetries
    *   Maximum number of retries on transient connection errors.
    * @param failureWindowSize
    *   Rolling window size for tracking success/failure outcomes.
    * @param failureThreshold
    *   Fraction of failures in the window (0.0–1.0) that triggers a throttle-down.
    * @param minSampleSize
    *   Minimum outcomes in the window before the failure rate is evaluated.
    * @param minRequestDelayMs
    *   Hard floor on inter-request spacing in milliseconds, applied inside `emaDelay`. Prevents burst behaviour at high
    *   permit counts when response times are unusually low. Set to 0 to disable.
    * @param minTierObservation
    *   Minimum wall-clock time that must elapse at a recovery tier before the tier is evaluated for promotion. Prevents
    *   premature step-ups when high concurrency fills the outcome window quickly.
    * @param emaTauMs
    *   Time constant in milliseconds for the response-time EMA's wall-clock exponential decay. Each new sample shifts
    *   the EMA by `1 - exp(-dt / emaTauMs)`, where `dt` is the elapsed time since the previous sample. A single slow
    *   outlier therefore decays out in real time rather than over a fixed number of subsequent samples — important
    *   for small-N sequential workloads where one slow response would otherwise dominate the run.
    */
  private[ccas] case class ThrottleConfig(
    recoveryTiers: Vector[Int],
    cooldown: Duration,
    cfCooldown: Duration,
    retryBase: Duration,
    cfRetryDelay: Duration,
    connectionRetryBase: Duration,
    max429Retries: Int,
    maxCfRetries: Int,
    maxConnectionRetries: Int,
    failureWindowSize: Int,
    failureThreshold: Double,
    minSampleSize: Int,
    minRequestDelayMs: Long,
    minTierObservation: Duration,
    emaTauMs: Long
  ) {
    require(
      recoveryTiers.nonEmpty && recoveryTiers.forall(_ >= 1) && recoveryTiers == recoveryTiers.sorted.distinct,
      s"recoveryTiers must be a non-empty strictly-increasing list of positive integers, got: $recoveryTiers"
    )
    require(!cooldown.isNegative, s"cooldown must be non-negative, got: $cooldown")
    require(!cfCooldown.isNegative, s"cfCooldown must be non-negative, got: $cfCooldown")
    require(!retryBase.isNegative && !retryBase.isZero, s"retryBase must be positive, got: $retryBase")
    require(!cfRetryDelay.isNegative, s"cfRetryDelay must be non-negative, got: $cfRetryDelay")
    require(
      !connectionRetryBase.isNegative && !connectionRetryBase.isZero,
      s"connectionRetryBase must be positive, got: $connectionRetryBase"
    )
    require(max429Retries >= 0, s"max429Retries must be >= 0, got: $max429Retries")
    require(maxCfRetries >= 0, s"maxCfRetries must be >= 0, got: $maxCfRetries")
    require(maxConnectionRetries >= 0, s"maxConnectionRetries must be >= 0, got: $maxConnectionRetries")
    require(failureWindowSize > 0, s"failureWindowSize must be positive, got: $failureWindowSize")
    require(minSampleSize > 0, s"minSampleSize must be positive, got: $minSampleSize")
    require(
      minSampleSize <= failureWindowSize,
      s"minSampleSize ($minSampleSize) must be <= failureWindowSize ($failureWindowSize)"
    )
    require(
      failureThreshold >= 0.0 && failureThreshold <= 1.0,
      s"failureThreshold must be in [0.0, 1.0], got: $failureThreshold"
    )
    require(minRequestDelayMs >= 0, s"minRequestDelayMs must be >= 0, got: $minRequestDelayMs")
    require(!minTierObservation.isNegative, s"minTierObservation must be non-negative, got: $minTierObservation")
    require(emaTauMs > 0, s"emaTauMs must be positive, got: $emaTauMs")

    val maxPermits: Long = recoveryTiers.last.toLong

    /** Return the next tier strictly greater than `currentMax`, or `maxPermits` if already at or above the top. */
    def nextTier(currentMax: Long): Long =
      recoveryTiers.find(_.toLong > currentMax).fold(maxPermits)(_.toLong)

    /** Return the previous tier strictly less than `currentMax`, or 1 if already at or below the first tier. */
    def previousTier(currentMax: Long): Long =
      recoveryTiers.findLast(_.toLong < currentMax).fold(1L)(_.toLong)
  }

  /** Raw config case class mapping 1:1 to HOCON keys under `chess-com-client`. Seconds fields are kept as Long to match
    * the plain-integer HOCON values (env var overrides are plain numbers); conversion to `Duration` happens in
    * `toThrottleConfig`.
    */
  @kebabCase
  private[ccas] case class ChessComClientConfig(
    contactEmail: String,
    recoveryTiers: Vector[Int],
    cooldownSeconds: Long,
    cfCooldownSeconds: Long,
    failureWindowSize: Int,
    failureThreshold: Double,
    minSampleSize: Int,
    minRequestDelayMs: Long,
    minTierObservationSeconds: Long,
    emaTauMs: Long,
    retryBaseSeconds: Long,
    cfRetryDelaySeconds: Long,
    connectionRetryBaseSeconds: Long,
    @name("max-429-retries") max429Retries: Int,
    maxCfRetries: Int,
    maxConnectionRetries: Int,
    statsFlushIntervalSeconds: Long
  )

  private[ccas] object ChessComClientConfig {
    given DeriveConfig[ChessComClientConfig] = DeriveConfig.derived

    extension (c: ChessComClientConfig) {
      def toThrottleConfig: ThrottleConfig = ThrottleConfig(
        recoveryTiers = c.recoveryTiers,
        cooldown = c.cooldownSeconds.seconds,
        cfCooldown = c.cfCooldownSeconds.seconds,
        retryBase = c.retryBaseSeconds.seconds,
        cfRetryDelay = c.cfRetryDelaySeconds.seconds,
        connectionRetryBase = c.connectionRetryBaseSeconds.seconds,
        max429Retries = c.max429Retries,
        maxCfRetries = c.maxCfRetries,
        maxConnectionRetries = c.maxConnectionRetries,
        failureWindowSize = c.failureWindowSize,
        failureThreshold = c.failureThreshold,
        minSampleSize = c.minSampleSize,
        minRequestDelayMs = c.minRequestDelayMs,
        minTierObservation = c.minTierObservationSeconds.seconds,
        emaTauMs = c.emaTauMs
      )

      def statsFlushInterval: Duration = c.statsFlushIntervalSeconds.seconds
    }
  }

  private def userAgentHeaders(contactEmail: String): Headers =
    Headers(
      Header.Custom("User-Agent", s"${BuildInfo.name.toUpperCase}/${BuildInfo.version} (contact: $contactEmail)"),
      Header.Accept(MediaType.application.json),
      Header.AcceptEncoding.GZip()
    )

  def live(appLabel: String): RLayer[Client & PostgresClient & ProgressDisplay & BodyStore, ChessComClient] =
    ZLayer.scoped {
      import ChessComClientConfig.*
      val provider = TypesafeConfigProvider.fromTypesafeConfig(
        ConfigFactory.load(), enableCommaSeparatedValueAsList = true
      )
      for {
        rawConfig      <- provider.load(summon[DeriveConfig[ChessComClientConfig]].desc.nested("chess-com-client"))
        throttleConfig <- ZIO.attempt(rawConfig.toThrottleConfig)
        statsFlushInterval = rawConfig.statsFlushInterval
        _ <- ZIO.attempt(require(
          !statsFlushInterval.isNegative && !statsFlushInterval.isZero,
          s"stats-flush-interval-seconds must be positive, got: $statsFlushInterval"
        ))
        client        <- ZIO.service[Client]
        pgClient      <- ZIO.service[PostgresClient]
        bodyStore     <- ZIO.service[BodyStore]
        display       <- ZIO.service[ProgressDisplay]
        stateRef      <- Ref.make(ThrottleState(throttleConfig.maxPermits, 0, Vector.empty))
        activeRef     <- Ref.make(0)
        rateLimitGate <- Semaphore.make(1)
        lastReqRef    <- Ref.make(0L)
        // Holds the single in-flight rate-limit recovery fiber (interrupt-and-replaced per throttle in `applyThrottle`);
        // the finalizer below interrupts whatever remains at shutdown.
        recoveryFiberRef <- Ref.make(Option.empty[Fiber.Runtime[Throwable, Unit]])
        startedAt     <- Clock.instant
        sessionId      = startedAt.toString.replace(":", "").replace("-", "")
        stats               <- Ref.make(ClientStatsAccumulator())
        configIdRef         <- Ref.make(Option.empty[Long])
        endedAtRef          <- Ref.make(Option.empty[Instant])
        bar                 <- display.addBarScoped
        refs = ThrottleRefs(stateRef, activeRef, rateLimitGate, lastReqRef)
        flushCtx =
          ClientStatsFlushContext(sessionId, appLabel, startedAt, stats, configIdRef, throttleConfig, stateRef, pgClient, endedAtRef)
        flushFiber <- ClientStatsPersistence.persistStats(flushCtx).repeat(Schedule.fixed(statsFlushInterval)).forkDaemon
        // Bounded, best-effort final flush so a dead DB pool can't wedge scope-close (#193): interruptFork (don't await
        // a periodic flush that may be parked in a blocking checkout — matching endSession's rationale), then a
        // disconnected 5s timeout so the doomed write unwinds in the background instead of blocking shutdown for the
        // full connectionTimeout x retry cycle.
        _ <- ZIO.addFinalizer(
          flushFiber.interruptFork *> ClientStatsPersistence.finalFlush(flushCtx).disconnect.timeout(5.seconds)
        )
        _ <- ZIO.addFinalizer(recoveryFiberRef.get.flatMap(ZIO.foreachDiscard(_)(_.interrupt)))
      } yield ChessComClient(
        client,
        pgClient,
        bodyStore,
        userAgentHeaders(rawConfig.contactEmail),
        refs,
        stats,
        bar,
        throttleConfig,
        recoveryFiberRef,
        ClientStatsPersistence.endSession(flushCtx, flushFiber)
      )
    }
}
