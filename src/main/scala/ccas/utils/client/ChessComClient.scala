package ccas.utils.client

import java.time.{Instant, ZoneOffset}

import ccas.utils.sql.PostgresClient
import com.typesafe.config.ConfigFactory
import io.netty.handler.codec.PrematureChannelClosureException
import zio.*
import zio.config.derivation.{kebabCase, name}
import zio.config.magnolia.DeriveConfig
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.*
import zio.http.Method.GET
import zio.json.JsonDecoder

import ccas.analysis.tables.{ApiFetchFailure, ApiResponseBody, ApiResponseCache, ClientConfig, ClientStats}
import ccas.analysis.tables.subtypes.ApiResponseBodyId
import ccas.info.BuildInfo
import ccas.utils.{CcasLogger, ProgressBar}
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
  *     on `StatsAccumulator` so the `requests` / `successes` / `failures` numbers continue to reflect only real
  *     Chess.com API load.
  *
  * Constructed via the `ChessComClient.live` ZLayer which reads configuration from `application.conf` under the
  * `chess-com-client` prefix.
  */
final class ChessComClient(
  client: Client,
  pgClient: PostgresClient,
  headers: Headers,
  logger: CcasLogger,
  throttle: ChessComClient.ThrottleRefs,
  statsRef: Ref[ChessComClient.StatsAccumulator],
  firstResponseLogged: Ref[Boolean],
  progressBar: ProgressBar,
  config: ChessComClient.ThrottleConfig,
  scope: Scope
) {
  import throttle.*

  // `ZClientAspect.followRedirects` treats every 3xx as a redirect, including 304 Not Modified, and fails on the
  // missing Location header. Return the 304 response as-is so conditional-GET revalidation works; other redirect
  // failures still propagate as errors.
  private val batchedClient = client.batched @@ ZClientAspect.followRedirects(3) { (resp, message) =>
    if (resp.status == Status.NotModified) ZIO.succeed(resp)
    else ZIO.fail(Exception(s"Redirect failed: $message"))
  }

  private def rawGet[T](url: URL, conditional: Option[ApiResponseCache])(
    using jsonDecoder: JsonDecoder[T]
  ): Task[CacheableResult[T]] = {
    val request = buildRequest(url, conditional)
    (for {
      tier <- stateRef.get.map(_.currentMax.toInt)
      _    <- statsRef.update(_.incAttemptAtTier(tier))
      response <- batchedClient(request).tapError { e =>
        ZIO.whenDiscard(isConnectionError(e))(statsRef.update(_.incConnectionErrors))
      }
      result <- handleResponse[T](url, conditional, response, tier)
    } yield result).tapError { e =>
      val (errorType, msg, body) = e match {
        case e: HttpStatusException => (e.getClass.getSimpleName, Some(e.statusCode.toString), Some(e.responseBody))
        case other                  => (other.getClass.getSimpleName, Option(other.getMessage), None)
      }
      ApiFetchFailure
        .insert(ApiFetchFailure(Instant.now(), url.encode, errorType, msg, body))
        .provideEnvironment(ZEnvironment(pgClient))
        .ignore
    }
  }

  /** Build the outgoing request, attaching `If-None-Match` and/or `If-Modified-Since` validators when a prior cache
    * entry is present. Sending both when both are available is safe and maximizes the chance of a 304 regardless of
    * which validator the origin prefers (RFC 7232 §6).
    *
    * Note: `If-None-Match` is attached via `Header.Custom` with the raw wire-format etag (quotes included). zio-http
    * 3.10.1's typed `Header.IfNoneMatch.ETags` strips quotes during render (see `IfNoneMatch.render` in zio-http
    * sources), which is RFC 7232–incorrect and produces a header the origin won't match. We read the etag the same
    * way, via `rawHeader`, so what's stored in `api_response_cache.etag` is already in wire format and can be echoed
    * back verbatim.
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
    tier: Int
  )(using jsonDecoder: JsonDecoder[T]): Task[CacheableResult[T]] = {
    if (response.status == Status.NotModified) {
      conditional match {
        case Some(meta) =>
          // 304 counts as a success for the failure window: the origin is reachable and willing to serve us.
          // The `true` argument keeps `recordOutcome`'s non-429 branch (same as any other non-rate-limit response).
          // We deliberately don't call `logFirstResponseEncoding` here — 304 has no body, so there's no encoding
          // to verify; the log will fire on the next 2xx response in this client's lifetime.
          recordOutcome(true) *>
            statsRef.update(_.incCacheRevalidation) *>
            ApiResponseCache.touch(url.encode, Instant.now())
              .provideEnvironment(ZEnvironment(pgClient))
              .as(CacheableResult.Revalidated(meta.bodyId, loadAndDecode[T](url, meta.bodyId)))
        case None =>
          ZIO.fail(Exception(s"Unexpected 304 Not Modified for non-conditional request: $url"))
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
            logFirstResponseEncoding(response, string) *>
              handleSuccessBody[T](url, conditional, response, string)
          } else {
            val errorUpdate =
              if (cfChallenge) statsRef.update(_.incCf403AtTier(tier))
              else if (response.status.code == 429) statsRef.update(_.incError429AtTier(tier))
              else statsRef.update(_.incErrorOther)
            errorUpdate *> ZIO.fail(HttpStatusException(response.status.code, url, errorBody))
          }
        outcomeEffect *> errorPath
      }
    }
  }

  /** Success path: extract cache-control headers, upsert the response body into the cache (unless `no-store`), and
    * return `IdenticalBody` when the new body deduped to the same `body_id` as the prior cache entry, otherwise
    * `Changed` with the eagerly-decoded value.
    */
  private def handleSuccessBody[T](
    url: URL,
    conditional: Option[ApiResponseCache],
    response: Response,
    string: String
  )(using jsonDecoder: JsonDecoder[T]): Task[CacheableResult[T]] = {
    val directives     = parseCacheDirectives(response)
    // Parse through the typed ETag header (validates wire format, preserves strong/weak), then re-render to wire
    // format for storage. The rendered form (`"abc"` or `W/"abc"`) is what we echo back on conditional requests —
    // see note on `buildRequest` above for why we can't use the typed `IfNoneMatch.ETags` for the echo path.
    val etagOpt        = response.header(Header.ETag).map(Header.ETag.render)
    val lastModOpt     = response.header(Header.LastModified).map(_.value.toInstant)
    val contentTypeOpt = response.header(Header.ContentType).map(_.mediaType.fullType)
    val upsertEffect: Task[Option[ApiResponseBodyId]] =
      if (directives.noStore) ZIO.succeed(None)
      else
        ApiResponseCache
          .upsertWithBody(
            url = url.encode,
            body = string,
            etag = etagOpt,
            lastModified = lastModOpt,
            maxAgeSeconds = directives.maxAgeSeconds,
            contentType = contentTypeOpt,
            fetchedAt = Instant.now()
          )
          .provideEnvironment(ZEnvironment(pgClient))
          .asSome
    upsertEffect.flatMap { newBodyIdOpt =>
      val decodeLazy = ZIO.fromEither(jsonDecoder.decodeJson(string)).mapError(JsonDecodingException(_))
      (newBodyIdOpt, conditional.map(_.bodyId)) match {
        case (Some(newBodyId), Some(oldBodyId)) if newBodyId == oldBodyId =>
          statsRef.update(_.incCacheHit).as(CacheableResult.IdenticalBody(newBodyId, decodeLazy))
        case _ =>
          statsRef.update(_.incCacheMiss) *> decodeLazy.map(CacheableResult.Changed(_))
      }
    }
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

  /** Lazy body-load + decode for `Fresh` and `Revalidated` results. On JSON decode failure (schema drift where an
    * old cached body no longer parses against a newer case class), invalidate the cache entry and refetch over the
    * wire via a recursive `get[T]` call. The recursive call sees no cache row and flows through the miss path to
    * produce a `Changed` value, so there is no infinite recursion.
    */
  private def loadAndDecode[T](url: URL, bodyId: ApiResponseBodyId)(using jsonDecoder: JsonDecoder[T]): Task[T] =
    ApiResponseBody
      .loadById(bodyId)
      .provideEnvironment(ZEnvironment(pgClient))
      .flatMap {
        case Some(body) => ZIO.fromEither(jsonDecoder.decodeJson(body)).mapError(JsonDecodingException(_))
        case None =>
          ApiResponseCache
            .invalidate(url.encode)
            .provideEnvironment(ZEnvironment(pgClient))
            .ignore *> get[T](url)
      }
      .catchSome { case _: JsonDecodingException =>
        ApiResponseCache
          .invalidate(url.encode)
          .provideEnvironment(ZEnvironment(pgClient))
          .ignore *> get[T](url)
      }

  /** True if a cached entry is still within its `Cache-Control: max-age` window. Entries without a `max-age` are
    * never fresh — we always revalidate (via conditional headers, or a full fetch if no validators are available).
    */
  private def isFresh(meta: ApiResponseCache, now: Instant): Boolean =
    meta.maxAgeSeconds.exists(maxAge => now.isBefore(meta.fetchedAt.plusSeconds(maxAge)))

  /** Wrap the existing gate / permit / latency-timing block around `rawGet`, parameterised by the optional
    * conditional cache entry. All throttle, retry, and error-recording machinery lives inside here.
    */
  private def gatedRawGet[T](url: URL, conditional: Option[ApiResponseCache])(
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
        (duration, result) <- rawGet(url, conditional).timed
        ms = duration.toMillis
        _ <- updateResponseTimeEma(ms)
        _ <- statsRef.update(_.recordLatency(ms).addActiveMs(ms))
      } yield result
    }

  /** Cache-aware entry point. Checks `api_response_cache` first; on a fresh hit (within `max-age`) returns a
    * `Fresh` result without a network call. Otherwise dispatches to the gated + retried `rawGet`, passing any prior
    * cache row so `If-None-Match` / `If-Modified-Since` validators can be attached. Callers that want to skip
    * downstream processing on unchanged data should use this directly; callers that just want `T` should use `get`.
    */
  def getCacheable[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[CacheableResult[T]] =
    ApiResponseCache
      .lookupMeta(url.encode)
      .provideEnvironment(ZEnvironment(pgClient))
      .flatMap {
        case Some(meta) if isFresh(meta, Instant.now()) =>
          statsRef
            .update(_.incCacheHit)
            .as(CacheableResult.Fresh(meta.bodyId, loadAndDecode[T](url, meta.bodyId)))
        case cachedOpt =>
          statsRef.update(_.incRequests) *> withRetries(gatedRawGet[T](url, cachedOpt))
      }

  def get[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] =
    getCacheable[T](url).flatMap(_.getValue)

  private def withRetries[T](effect: Task[T]): Task[T] =
    effect
      .retry(retry429Schedule)
      .retry(retryCfSchedule)
      .retry(retryConnectionSchedule)
      .tapBoth(
        _ => statsRef.update(_.incFailures),
        _ => statsRef.update(_.incSuccesses)
      )

  def getAll[T](urls: Iterable[URL])(using jsonDecoder: JsonDecoder[T]): Task[Chunk[T]] =
    ZIO.foreachPar(Chunk.from(urls))(get)

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
    if (config.maxPermits <= 1) ZIO.unit
    else stateRef.get.flatMap { state =>
      ZIO.unlessDiscard(state.responseTimeEma <= 0) {
        val targetDelay = math.max((state.responseTimeEma / state.currentMax).toLong, config.minRequestDelayMs)
        for {
          now  <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
          last <- lastRequestRef.get
          gap = now - last
          _ <- ZIO.whenDiscard(gap < targetDelay)(ZIO.sleep((targetDelay - gap).millis))
          _ <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap(lastRequestRef.set)
        } yield ()
      }
    }

  private val emaAlpha = 0.1

  private def updateResponseTimeEma(responseMs: Long): UIO[Unit] =
    stateRef.update { state =>
      val ema = state.responseTimeEma
      val newEma = if (ema <= 0) responseMs.toDouble else emaAlpha * responseMs + (1 - emaAlpha) * ema
      state.copy(responseTimeEma = newEma)
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
    Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap { now =>
      stateRef.modify { state =>
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
    }.flatMap {
      case None => ZIO.unit
      case Some((oldMax, gen)) =>
        statsRef.update(_.incThrottleDowns) *>
          logger.warn(s"Rate limit throttle: $oldMax \u2192 1 permit") *>
          scheduleRecovery(gen, cooldown).forkDaemon
            .flatMap(f => scope.addFinalizerExit(_ => f.interrupt)).unit
    }

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
            responseTimeEma = if (fullyRecovered) 0.0 else state.responseTimeEma
          )
          (Some((state.currentMax, newMax, generation, throttleDuration)), newState)
        }
      }
      _ <- ZIO.foreachDiscard(option) { case (oldMax, newMax, gen, throttleDuration) =>
        ZIO.whenDiscard(throttleDuration > 0)(statsRef.update(_.addThrottled(throttleDuration))) *> {
            if (newMax == oldMax) {
              scheduleRecovery(gen, cooldown)
            } else if (newMax < oldMax) {
              logger.warn(s"Rate limit dropping back: $oldMax \u2192 $newMax permit(s)") *>
                scheduleRecovery(gen, cooldown)
            } else {
              val msg =
                if (newMax == config.maxPermits) "Rate limit throttle lifted"
                else s"Rate limit easing: $oldMax \u2192 $newMax permits"
              logger.info(msg) *>
                ZIO.unlessDiscard(newMax == config.maxPermits)(scheduleRecovery(gen, cooldown))
            }
          }
      }
    } yield ()

  private val CfChallengeMarker = "/cdn-cgi/challenge-platform/"

  private def isCloudflareChallenge(response: Response, body: String): Boolean =
    response.status == Status.Forbidden && body.contains(CfChallengeMarker)

  /** One-shot debug log: on the first successful response of this client's lifetime, report whether the server used
    * gzip encoding and the decoded body size. Confirms that `Accept-Encoding: gzip` is being honored by Chess.com and
    * that zio-http's `Decompression.NonStrict` pipeline is decoding transparently. Subsequent responses are silent.
    */
  private def logFirstResponseEncoding(response: Response, body: String): UIO[Unit] =
    firstResponseLogged.getAndSet(true).flatMap { alreadyLogged =>
      ZIO.unlessDiscard(alreadyLogged) {
        val encoding = response.header(Header.ContentEncoding).map(_.encoding).getOrElse("none")
        logger.info(s"First API response: Content-Encoding=$encoding, decoded body size=${body.length} chars")
      }
    }

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

  private def isConnectionError(e: Throwable): Boolean = e match {
    case _: HttpStatusException              => false
    case _: JsonDecodingException            => false
    case _: java.io.IOException              => true
    case _: PrematureChannelClosureException => true
    case _                                   => false
  }

  private val retryConnectionSchedule: Schedule[Any, Throwable, Any] =
    Schedule.exponential(config.connectionRetryBase) && Schedule.recurs(config.maxConnectionRetries) &&
      Schedule.recurWhile[Throwable](isConnectionError)
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
    responseTimeEma: Double = 0.0
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
    minTierObservation: Duration
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
        minTierObservation = c.minTierObservationSeconds.seconds
      )

      def statsFlushInterval: Duration = c.statsFlushIntervalSeconds.seconds
    }
  }

  /** Upper-exclusive boundaries for latency histogram buckets, in milliseconds. A latency < 50 lands in bucket 0, a
    * latency in [50, 100) lands in bucket 1, etc. Values >= 1000 land in the final overflow bucket.
    */
  private val LatencyBuckets: Array[Long] = Array(50, 100, 200, 500, 1000)

  /** Bucket count: one per boundary plus an overflow bucket for values >= the largest boundary. */
  private val LatencyBucketCount: Int = LatencyBuckets.length + 1

  private[ccas] object StatsAccumulator {
    /** Serialize a tier-keyed counter map as a sorted pipe-delimited string: `"tier:count|tier:count|..."`. */
    def serializeTierMap(m: Map[Int, Long]): String =
      m.toVector.sortBy(_._1).map((k, v) => s"$k:$v").mkString("|")
  }

  private[ccas] case class StatsAccumulator(
    requests: Long = 0,
    successes: Long = 0,
    failures: Long = 0,
    attempts: Long = 0,
    errors429: Long = 0,
    errorsCf403: Long = 0,
    errorsOther: Long = 0,
    connectionErrors: Long = 0,
    throttleDowns: Long = 0,
    peakConcurrent: Int = 0,
    latencyMinMs: Long = Long.MaxValue,
    latencyMaxMs: Long = 0,
    latencySumMs: Long = 0,
    latencyCount: Long = 0,
    latencyBuckets: Vector[Long] = Vector.fill(LatencyBucketCount)(0L),
    gateWaitMs: Long = 0,
    emaDelayMs: Long = 0,
    activeMs: Long = 0,
    throttledMs: Long = 0,
    attemptsByTier: Map[Int, Long] = Map.empty,
    errors429ByTier: Map[Int, Long] = Map.empty,
    errorsCf403ByTier: Map[Int, Long] = Map.empty,
    cacheHits: Long = 0,
    cacheRevalidations: Long = 0,
    cacheMisses: Long = 0
  ) {
    def incRequests: StatsAccumulator           = copy(requests = requests + 1)
    def incSuccesses: StatsAccumulator          = copy(successes = successes + 1)
    def incFailures: StatsAccumulator           = copy(failures = failures + 1)
    def incConnectionErrors: StatsAccumulator   = copy(connectionErrors = connectionErrors + 1)
    def incThrottleDowns: StatsAccumulator      = copy(throttleDowns = throttleDowns + 1)
    def incCacheHit: StatsAccumulator           = copy(cacheHits = cacheHits + 1)
    def incCacheRevalidation: StatsAccumulator  = copy(cacheRevalidations = cacheRevalidations + 1)
    def incCacheMiss: StatsAccumulator          = copy(cacheMisses = cacheMisses + 1)

    /** True if this session did anything worth persisting. `requests` covers every network request
      * (cacheMisses and cacheRevalidations both ride along with `incRequests`); `cacheHits` catches the one case
      * where a session did only Fresh max-age hits and never touched the network.
      */
    def hasActivity: Boolean = requests > 0 || cacheHits > 0
    /** Record a Cloudflare challenge 403 at the given permit tier, incrementing both the total and per-tier counters. */
    def incCf403AtTier(tier: Int): StatsAccumulator =
      copy(
        errorsCf403 = errorsCf403 + 1,
        errorsCf403ByTier = errorsCf403ByTier.updated(tier, errorsCf403ByTier.getOrElse(tier, 0L) + 1)
      )
    def updatePeak(n: Int): StatsAccumulator  = copy(peakConcurrent = peakConcurrent.max(n))
    def addGateWait(ms: Long): StatsAccumulator = copy(gateWaitMs = gateWaitMs + ms)
    def addEmaDelay(ms: Long): StatsAccumulator = copy(emaDelayMs = emaDelayMs + ms)
    def addActiveMs(ms: Long): StatsAccumulator = copy(activeMs = activeMs + ms)
    def addThrottled(ms: Long): StatsAccumulator = copy(throttledMs = throttledMs + ms)

    /** Increment the attempts counter AND the per-tier attempts counter for the current permit level. */
    def incAttemptAtTier(tier: Int): StatsAccumulator =
      copy(
        attempts = attempts + 1,
        attemptsByTier = attemptsByTier.updated(tier, attemptsByTier.getOrElse(tier, 0L) + 1)
      )

    /** Record a 429 error at the given permit tier, incrementing both the total and per-tier counters. */
    def incError429AtTier(tier: Int): StatsAccumulator =
      copy(
        errors429 = errors429 + 1,
        errors429ByTier = errors429ByTier.updated(tier, errors429ByTier.getOrElse(tier, 0L) + 1)
      )

    /** Record a non-rate-limit error. 429s go through `incError429AtTier` and Cloudflare 403s through `incCf403`;
      * everything else (404, plain 403, 5xx, etc.) is counted here.
      */
    def incErrorOther: StatsAccumulator =
      copy(errorsOther = errorsOther + 1)

    def recordLatency(ms: Long): StatsAccumulator = {
      val idx = LatencyBuckets.indexWhere(ms < _) match {
        case -1 => LatencyBuckets.length
        case i  => i
      }
      copy(
        latencyMinMs = latencyMinMs.min(ms),
        latencyMaxMs = latencyMaxMs.max(ms),
        latencySumMs = latencySumMs + ms,
        latencyCount = latencyCount + 1,
        latencyBuckets = latencyBuckets.updated(idx, latencyBuckets(idx) + 1)
      )
    }

    def toClientStats(
      sessionId: String,
      appLabel: String,
      startedAt: Instant,
      completedAt: Instant,
      configId: Long,
      inProgressThrottleMs: Long
    ): ClientStats = {
      val minDisplay  = if (latencyMinMs == Long.MaxValue) 0L else latencyMinMs
      val meanLatency = if (latencyCount > 0) latencySumMs / latencyCount else 0L
      ClientStats(
        sessionId = sessionId,
        appLabel = appLabel,
        configId = configId,
        startedAt = startedAt,
        completedAt = completedAt,
        requests = requests,
        successes = successes,
        failures = failures,
        attempts = attempts,
        attemptsByTier = StatsAccumulator.serializeTierMap(attemptsByTier),
        errors429 = errors429,
        errors429ByTier = StatsAccumulator.serializeTierMap(errors429ByTier),
        errorsCf403 = errorsCf403,
        errorsCf403ByTier = StatsAccumulator.serializeTierMap(errorsCf403ByTier),
        errorsOther = errorsOther,
        connectionErrors = connectionErrors,
        throttleDowns = throttleDowns,
        throttledMs = throttledMs + inProgressThrottleMs,
        peakConcurrent = peakConcurrent,
        activeMs = activeMs,
        gateWaitMs = gateWaitMs,
        emaDelayMs = emaDelayMs,
        latencyMinMs = minDisplay,
        latencyMaxMs = latencyMaxMs,
        latencyMeanMs = meanLatency,
        latencyBucket0To50 = latencyBuckets(0),
        latencyBucket50To100 = latencyBuckets(1),
        latencyBucket100To200 = latencyBuckets(2),
        latencyBucket200To500 = latencyBuckets(3),
        latencyBucket500To1000 = latencyBuckets(4),
        latencyBucket1000Plus = latencyBuckets(5),
        cacheHits = cacheHits,
        cacheRevalidations = cacheRevalidations,
        cacheMisses = cacheMisses
      )
    }

    def summary: String = {
      val retries        = attempts - requests
      val meanLatency    = if (latencyCount > 0) latencySumMs / latencyCount else 0L
      val minDisplay     = if (latencyMinMs == Long.MaxValue) 0L else latencyMinMs
      val failedSuffix   = if (failures > 0) s" ($failures failed)" else ""
      val retrySuffix    = if (retries > 0) s", $retries retries" else ""
      val throttleSuffix = if (throttleDowns > 0) s", $throttleDowns throttle-downs" else ""
      val latencySuffix =
        if (latencyCount > 0) s", latency min/mean/max = $minDisplay/$meanLatency/${latencyMaxMs}ms" else ""
      val overheadParts = List(
        if (gateWaitMs > 0) Some(s"gate=${gateWaitMs}ms") else None,
        if (emaDelayMs > 0) Some(s"ema=${emaDelayMs}ms") else None,
        if (throttledMs > 0) Some(s"throttled=${throttledMs}ms") else None
      ).flatten
      val overheadSuffix = if (overheadParts.nonEmpty) s", overhead ${overheadParts.mkString(", ")}" else ""
      val cacheSuffix =
        if (cacheHits > 0 || cacheRevalidations > 0 || cacheMisses > 0)
          s", cache: $cacheHits hits / $cacheRevalidations revalidated / $cacheMisses misses"
        else ""
      s"API stats: $requests requests$failedSuffix$retrySuffix$throttleSuffix$latencySuffix$overheadSuffix$cacheSuffix"
    }
  }

  /** Bundles the refs and config needed by periodic and final stats flushes. Created once in `live` and passed to
    * `persistStats` / `finalFlush`.
    */
  private[ccas] case class FlushContext(
    sessionId: String,
    appLabel: String,
    startedAt: Instant,
    statsRef: Ref[StatsAccumulator],
    configIdRef: Ref[Option[Long]],
    config: ThrottleConfig,
    stateRef: Ref[ThrottleState],
    pgClient: PostgresClient
  )

  /** Upsert the cumulative stats snapshot for this session. Each call overwrites the single row for this session,
    * creating it on first flush. On first insert, also inserts the `ClientConfig` row. Silently swallows DB errors
    * so it is safe to call from finalizers and background fibers.
    */
  private[ccas] def persistStats(ctx: FlushContext): UIO[Unit] =
    (for {
      current <- ctx.statsRef.get
      _ <- ZIO.whenDiscard(current.hasActivity) {
        for {
          now            <- Clock.instant
          inProgress <- inProgressThrottleMs(ctx.stateRef)
          configId <- ctx.configIdRef.get.flatMap {
            case Some(id) => ZIO.succeed(id)
            case None =>
              ClientConfig.ensureConfig(toClientConfig(ctx.config))
                .provideEnvironment(ZEnvironment(ctx.pgClient))
                .tap(id => ctx.configIdRef.set(Some(id)))
          }
          row = current.toClientStats(ctx.sessionId, ctx.appLabel, ctx.startedAt, now, configId, inProgress)
          _ <- ClientStats.upsert(row).provideEnvironment(ZEnvironment(ctx.pgClient))
        } yield ()
      }
    } yield ()).ignore

  private def toClientConfig(config: ThrottleConfig): ClientConfig = {
    val cc = ClientConfig(
      configId = 0,
      configHash = "",
      recoveryTiers = config.recoveryTiers.toList,
      minRequestDelayMs = config.minRequestDelayMs,
      cooldownSecs = config.cooldown.getSeconds.toInt,
      cfCooldownSecs = config.cfCooldown.getSeconds.toInt,
      minTierObservationSecs = config.minTierObservation.getSeconds.toInt,
      failureWindowSize = config.failureWindowSize,
      failureThreshold = config.failureThreshold,
      minSampleSize = config.minSampleSize,
      retryBaseSecs = config.retryBase.getSeconds.toInt,
      cfRetrySecs = config.cfRetryDelay.getSeconds.toInt,
      connectionRetryBaseSecs = config.connectionRetryBase.getSeconds.toInt,
      max429Retries = config.max429Retries,
      maxCfRetries = config.maxCfRetries,
      maxConnectionRetries = config.maxConnectionRetries
    )
    cc.copy(configHash = cc.computeHash)
  }

  private def inProgressThrottleMs(stateRef: Ref[ThrottleState]): UIO[Long] =
    Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap { now =>
      stateRef.get.map(_.throttledSince.fold(0L)(now - _))
    }

  /** Final stats flush: upserts the cumulative snapshot, then logs a summary. Called by the scope finalizer. */
  private def finalFlush(ctx: FlushContext, logger: CcasLogger): UIO[Unit] =
    ctx.statsRef.get.flatMap { cumulative =>
      persistStats(ctx) *>
        ZIO.whenDiscard(cumulative.hasActivity)(logger.info(cumulative.summary))
    }

  private def userAgentHeaders(contactEmail: String): Headers =
    Headers(
      Header.Custom("User-Agent", s"${BuildInfo.name.toUpperCase}/${BuildInfo.version} (contact: $contactEmail)"),
      Header.Accept(MediaType.application.json),
      Header.AcceptEncoding.GZip()
    )

  def live(appLabel: String): RLayer[Client & PostgresClient & CcasLogger, ChessComClient] =
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
        clientScope   <- ZIO.service[Scope]
        client        <- ZIO.service[Client]
        pgClient      <- ZIO.service[PostgresClient]
        logger        <- ZIO.service[CcasLogger]
        stateRef      <- Ref.make(ThrottleState(throttleConfig.maxPermits, 0, Vector.empty))
        activeRef     <- Ref.make(0)
        rateLimitGate <- Semaphore.make(1)
        lastReqRef    <- Ref.make(0L)
        startedAt     <- Clock.instant
        sessionId      = startedAt.toString.replace(":", "").replace("-", "")
        stats               <- Ref.make(StatsAccumulator())
        firstResponseLogged <- Ref.make(false)
        configIdRef         <- Ref.make(Option.empty[Long])
        bar                 <- logger.progressBar
        refs = ThrottleRefs(stateRef, activeRef, rateLimitGate, lastReqRef)
        flushCtx = FlushContext(sessionId, appLabel, startedAt, stats, configIdRef, throttleConfig, stateRef, pgClient)
        flushFiber <- persistStats(flushCtx).repeat(Schedule.fixed(statsFlushInterval)).forkDaemon
        _ <- ZIO.addFinalizer(flushFiber.interrupt *> finalFlush(flushCtx, logger))
      } yield ChessComClient(
        client,
        pgClient,
        userAgentHeaders(rawConfig.contactEmail),
        logger,
        refs,
        stats,
        firstResponseLogged,
        bar,
        throttleConfig,
        clientScope
      )
    }
}
