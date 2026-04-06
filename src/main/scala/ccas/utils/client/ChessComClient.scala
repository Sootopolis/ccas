package ccas.utils.client

import java.time.Instant

import ccas.utils.sql.PostgresClient
import com.typesafe.config.ConfigFactory
import io.netty.handler.codec.PrematureChannelClosureException
import zio.*
import zio.http.*
import zio.http.Method.GET
import zio.json.JsonDecoder

import ccas.analysis.tables.{ApiFetchFailure, ApiResponseBody, ClientConfig, ClientStats}
import ccas.info.BuildInfo
import ccas.utils.{CcasLogger, ProgressBar}
import ccas.utils.json.JsonDecodingException

/** HTTP client for the Chess.com public API with adaptive rate limiting.
  *
  * Wraps a `zio-http` `Client` and adds several layers of concurrency and error management:
  *
  *   - '''Gate-based admission control''' — a single-permit gate serializes request admission. Before each request, the
  *     gate checks that the number of in-flight requests (`activeRef`) is below `currentMax`. If not, it polls until a
  *     slot opens. This enforces the concurrency limit without a semaphore, so throttle-down takes effect immediately
  *     for all requests that haven't yet entered the gate. The gate wait is interruptible so that pending requests can
  *     be cancelled promptly on shutdown; only the fast `activeRef` increment is uninterruptible.
  *   - '''EMA-based rate delay''' — tracks an exponential moving average of response times and staggers outgoing
  *     requests so that the full permit budget is utilised without bursting. When permits are reduced the per-request
  *     delay grows proportionally.
  *   - '''Failure-window throttle-down''' — maintains a rolling window of success/failure outcomes. HTTP 429 and
  *     transient connection errors count as failures; non-rate-limit responses (403 non-CF, 404, 500, etc.) count
  *     as non-failures. Cloudflare challenge 403s bypass the window and trigger an immediate hard throttle to 1
  *     permit. When the failure rate in the window exceeds a configurable threshold, `currentMax` drops to the
  *     lowest recovery tier and the gate immediately enforces it.
  *   - '''Generation-gated recovery''' — after a cooldown period, a background fiber walks `currentMax` up through the
  *     configured `recoveryTiers` (one step per cooldown cycle), or holds if failures persist. `coolingDown` stays
  *     true throughout the recovery ladder to prevent `recordOutcome` from triggering additional throttle-downs
  *     mid-recovery; it is only cleared when permits reach `maxPermits`. A generation counter ensures that only the
  *     most recent throttle-down triggers recovery, preventing stale fibers from interfering. The response-time EMA
  *     is reset to zero on full recovery so that stale inflated values do not gate post-recovery request pacing.
  *     Recovery fibers are scoped to the client's lifetime and interrupted when the layer is torn down.
  *   - '''Retry schedules''' — separate schedules handle HTTP 429 (exponential backoff), Cloudflare challenge 403s
  *     (fixed delay), and transient connection errors (exponential backoff); each has an independent retry-count
  *     budget configured via `max-429-retries`, `max-cf-retries`, and `max-connection-retries`. Non-Cloudflare 403
  *     and HTTP 404 are treated as permanent and never retried.
  *   - '''Cumulative stats flushing''' — a background daemon fiber upserts a single `client_stats` row per session
  *     every `stats-flush-interval-seconds`, overwriting the previous snapshot with cumulative totals. The throttle
  *     configuration is inserted once into `client_config` on first flush and referenced by FK. A final flush runs
  *     in the scope finalizer to ensure stats survive non-graceful shutdowns with at most one interval's worth of
  *     data loss.
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
  progressBar: ProgressBar,
  config: ChessComClient.ThrottleConfig,
  scope: Scope
) {
  import throttle.*

  private val batchedClient = client.batched @@ ZClientAspect.followRedirects(3) { (_, message) =>
    ZIO.fail(Exception(s"Redirect failed: $message"))
  }

  private def rawGet[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] = (for {
    tier <- stateRef.get.map(_.currentMax.toInt)
    _ <- statsRef.update(_.incAttemptAtTier(tier))
    response <- batchedClient(Request(method = GET, url = url).addHeaders(headers)).tapError { e =>
      ZIO.whenDiscard(isConnectionError(e))(
        statsRef.update(_.incConnectionErrors) *> recordOutcome(false)
      )
    }
    string <- response.body.asString
    cfChallenge = isCloudflareChallenge(response, string)
    _ <-
      if (cfChallenge) throttleDown(config.cfCooldown, floor = 1)
      else recordOutcome(response.status != Status.TooManyRequests)
    errorBody = if (cfChallenge) ApiResponseBody.CfCanonicalBody else string
    _ <- ZIO.whenDiscard(!response.status.isSuccess) {
      val errorUpdate =
        if (cfChallenge) statsRef.update(_.incCf403)
        else if (response.status.code == 429) statsRef.update(_.incError429AtTier(tier))
        else statsRef.update(_.incError(response.status.code))
      errorUpdate *> ZIO.fail(HttpStatusException(response.status.code, url, errorBody))
    }
    value <- ZIO.fromEither(jsonDecoder.decodeJson(string)).mapError(JsonDecodingException(_))
  } yield value).tapError { e =>
    val (errorType, msg, body) = e match {
      case e: HttpStatusException => (e.getClass.getSimpleName, Some(e.statusCode.toString), Some(e.responseBody))
      case other                  => (other.getClass.getSimpleName, Option(other.getMessage), None)
    }
    ApiFetchFailure
      .insert(ApiFetchFailure(Instant.now(), url.encode, errorType, msg, body))
      .provideEnvironment(ZEnvironment(pgClient))
      .ignore
  }

  def get[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] =
    statsRef.update(_.incRequests) *> withRetries {
      for {
        _ <- rateLimitGate.withPermit {
          for {
            (gateWait, _) <- awaitCapacity.timed
            (emaWait, _)  <- emaDelay.timed
            _             <- statsRef.update(_.addGateWait(gateWait.toMillis).addEmaDelay(emaWait.toMillis))
          } yield ()
        }
        (duration, result) <- ZIO.acquireReleaseWith(activeRef.updateAndGet(_ + 1).tap(updateBar))(_ => releaseSlot()) {
          active => statsRef.update(_.updatePeak(active)) *> rawGet(url).timed
        }
        ms = duration.toMillis
        _ <- updateResponseTimeEma(ms) *> statsRef.update(_.recordLatency(ms).addActiveMs(ms))
      } yield result
    }

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

  /** Poll until an active slot is available. Runs inside the serializing admission gate; the gate is released before
    * the request starts so that admission is interruptible — only the fast `activeRef` increment that follows is
    * uninterruptible (via `acquireReleaseWith` in `get`).
    */
  private def awaitCapacity: Task[Unit] =
    (for {
      active <- activeRef.get
      state  <- stateRef.get
    } yield active < state.currentMax.toInt)
      .repeat(Schedule.spaced(10.millis) && Schedule.recurWhile(!_)).unit

  private def emaDelay: Task[Unit] =
    responseTimeEma.get.flatMap { ema =>
      ZIO.unlessDiscard(ema <= 0) {
        stateRef.get.flatMap { state =>
          val targetDelay = (ema / state.currentMax).toLong
          for {
            now  <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
            last <- lastRequestRef.get
            gap = now - last
            _ <- ZIO.whenDiscard(gap < targetDelay)(ZIO.sleep((targetDelay - gap).millis))
            _ <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap(lastRequestRef.set)
          } yield ()
        }
      }
    }

  private val emaAlpha = 0.1

  private def updateResponseTimeEma(responseMs: Long): UIO[Unit] =
    responseTimeEma.update { ema =>
      if (ema <= 0) responseMs.toDouble
      else emaAlpha * responseMs + (1 - emaAlpha) * ema
    }

  /** Record a request outcome and trigger throttle-down if failure rate exceeds threshold. Skipped while cooling down
    * (after a recent throttle-down) to prevent cascading reductions from a single burst of failures.
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

  /** Drop the effective concurrency limit to `floor` and schedule drain-then-recover. Failure-rate throttles default
    * to `recoveryTiers.head`; Cloudflare challenges pass `floor = 1` for a hard stop. Clears the outcome window and
    * sets coolingDown to prevent cascading reductions. The `cooldown` parameter controls how long to wait after
    * in-flight requests have drained before attempting recovery.
    */
  private def throttleDown(cooldown: Duration = config.cooldown, floor: Long = config.recoveryTiers.head.toLong): Task[Unit] =
    Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap { now =>
      stateRef.modify { state =>
        if (state.currentMax <= floor) {
          (None, state)
        } else {
          val newGen = state.generation + 1
          val newState = state.copy(
            currentMax = floor,
            generation = newGen,
            outcomes = Vector.empty,
            coolingDown = true,
            throttledSince = Some(now)
          )
          (Some((state.currentMax, newGen)), newState)
        }
      }
    }.flatMap {
      case None => ZIO.unit
      case Some((oldMax, gen)) =>
        val permitLabel = if (floor == 1) "1 permit" else s"$floor permits"
        statsRef.update(_.incThrottleDowns) *>
          logger.warn(s"Rate limit throttle: $oldMax \u2192 $permitLabel") *>
          scheduleRecovery(gen, cooldown).forkDaemon
            .flatMap(f => scope.addFinalizerExit(_ => f.interrupt)).unit
    }

  /** Wait for in-flight requests to drain, sleep for cooldown, then recover permits if failure rate has dropped. Keeps
    * coolingDown true throughout the recovery ladder to prevent mid-recovery re-throttling; clears it only when permits
    * reach maxPermits. Resets the response-time EMA on full recovery.
    */
  private def scheduleRecovery(generation: Long, cooldown: Duration): Task[Unit] =
    for {
      _ <- activeRef.get.repeat(Schedule.spaced(200.millis) && Schedule.recurWhile(_ > 1)).unit
      _ <- ZIO.sleep(cooldown)
      now <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
      option <- stateRef.modify { state =>
        if (state.generation != generation) (None, state)
        else if (failureRate(state.outcomes) > config.failureThreshold) {
          (Some((state.currentMax, state.currentMax, generation, 0L)), state)
        } else {
          val newMax        = config.nextTier(state.currentMax)
          val clearOutcomes = newMax == config.maxPermits
          val throttleDuration =
            if (clearOutcomes) state.throttledSince.fold(0L)(now - _)
            else 0L
          val newState = state.copy(
            currentMax = newMax,
            coolingDown = !clearOutcomes,
            outcomes = if (clearOutcomes) Vector.empty else state.outcomes,
            throttledSince = if (clearOutcomes) None else state.throttledSince
          )
          (Some((state.currentMax, newMax, generation, throttleDuration)), newState)
        }
      }
      _ <- ZIO.foreachDiscard(option) { case (oldMax, newMax, gen, throttleDuration) =>
        ZIO.whenDiscard(throttleDuration > 0)(statsRef.update(_.addThrottled(throttleDuration))) *>
          ZIO.whenDiscard(newMax == config.maxPermits)(responseTimeEma.set(0.0)) *> {
            if (oldMax == newMax) {
              logger.warn(s"Rate limit still elevated, holding at $newMax permit(s)") *> scheduleRecovery(gen, cooldown)
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
  private[ccas] case class ThrottleRefs(
    stateRef: Ref[ThrottleState],
    activeRef: Ref[Int],
    rateLimitGate: Semaphore,
    lastRequestRef: Ref[Long],
    responseTimeEma: Ref[Double]
  )

  private[ccas] case class ThrottleState(
    currentMax: Long,
    generation: Long,
    outcomes: Vector[Boolean],
    coolingDown: Boolean = false,
    throttledSince: Option[Long] = None
  )

  /** @param recoveryTiers
    *   Strictly-increasing permit counts (all >= 2) that define the recovery ladder. Failure-rate throttle-downs drop
    *   to the first tier; Cloudflare throttle-downs drop to 1. Recovery walks up the tiers until reaching the last
    *   value, which is the maximum permit count (`maxPermits`).
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
    minSampleSize: Int
  ) {
    require(
      recoveryTiers.nonEmpty && recoveryTiers.forall(_ >= 2) && recoveryTiers == recoveryTiers.sorted.distinct,
      s"recoveryTiers must be a non-empty strictly-increasing list of integers >= 2, got: $recoveryTiers"
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

    val maxPermits: Long = recoveryTiers.last.toLong

    /** Return the next tier strictly greater than `currentMax`, or `maxPermits` if already at or above the top. */
    def nextTier(currentMax: Long): Long =
      recoveryTiers.find(_.toLong > currentMax).fold(maxPermits)(_.toLong)
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
    errors404: Long = 0,
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
    errors429ByTier: Map[Int, Long] = Map.empty
  ) {
    def incRequests: StatsAccumulator         = copy(requests = requests + 1)
    def incSuccesses: StatsAccumulator        = copy(successes = successes + 1)
    def incFailures: StatsAccumulator         = copy(failures = failures + 1)
    def incConnectionErrors: StatsAccumulator = copy(connectionErrors = connectionErrors + 1)
    def incThrottleDowns: StatsAccumulator    = copy(throttleDowns = throttleDowns + 1)
    def incCf403: StatsAccumulator            = copy(errorsCf403 = errorsCf403 + 1)
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

    /** Record a non-rate-limit error by status code. Only 404s are tracked; 429s go through `incError429AtTier`,
      * Cloudflare 403s through `incCf403`, and other codes (plain 403, 5xx) are not counted individually.
      */
    def incError(statusCode: Int): StatsAccumulator = statusCode match {
      case 404 => copy(errors404 = errors404 + 1)
      case _   => this
    }

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
      currentPermits: Int,
      inProgressThrottleMs: Long
    ): ClientStats = {
      val minDisplay     = if (latencyMinMs == Long.MaxValue) 0L else latencyMinMs
      val meanLatency    = if (latencyCount > 0) latencySumMs / latencyCount else 0L
      val activeSecs     = activeMs / 1000.0
      val requestsPerSec = if (activeSecs > 0) successes.toDouble / activeSecs else 0.0
      ClientStats(
        sessionId = sessionId,
        appLabel = appLabel,
        configId = configId,
        startedAt = startedAt,
        completedAt = completedAt,
        requestsPerSec = requestsPerSec,
        activeMs = activeMs,
        requests = requests,
        successes = successes,
        failures = failures,
        attempts = attempts,
        attemptsByTier = StatsAccumulator.serializeTierMap(attemptsByTier),
        errors429 = errors429,
        errors429ByTier = StatsAccumulator.serializeTierMap(errors429ByTier),
        errorsCf403 = errorsCf403,
        errors404 = errors404,
        connectionErrors = connectionErrors,
        throttleDowns = throttleDowns,
        throttledMs = throttledMs + inProgressThrottleMs,
        currentPermits = currentPermits,
        peakConcurrent = peakConcurrent,
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
        latencyBucket1000Plus = latencyBuckets(5)
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
      s"API stats: $requests requests$failedSuffix$retrySuffix$throttleSuffix$latencySuffix$overheadSuffix"
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
      _ <- ZIO.whenDiscard(current.requests > 0) {
        for {
          now            <- Clock.instant
          inProgress     <- inProgressThrottleMs(ctx.stateRef)
          currentPermits <- ctx.stateRef.get.map(_.currentMax.toInt)
          configId <- ctx.configIdRef.get.flatMap {
            case Some(id) => ZIO.succeed(id)
            case None =>
              ClientConfig.ensureConfig(toClientConfig(ctx.config))
                .provideEnvironment(ZEnvironment(ctx.pgClient))
                .tap(id => ctx.configIdRef.set(Some(id)))
          }
          row = current.toClientStats(ctx.sessionId, ctx.appLabel, ctx.startedAt, now, configId, currentPermits, inProgress)
          _ <- ClientStats.upsert(row).provideEnvironment(ZEnvironment(ctx.pgClient))
        } yield ()
      }
    } yield ()).ignore

  private def toClientConfig(config: ThrottleConfig): ClientConfig = {
    val cc = ClientConfig(
      configId = 0,
      configHash = "",
      recoveryTiers = config.recoveryTiers.toList,
      cooldownSecs = config.cooldown.getSeconds.toInt,
      cfCooldownSecs = config.cfCooldown.getSeconds.toInt,
      retryBaseSecs = config.retryBase.getSeconds.toInt,
      cfRetrySecs = config.cfRetryDelay.getSeconds.toInt,
      connectionRetryBaseSecs = config.connectionRetryBase.getSeconds.toInt,
      max429Retries = config.max429Retries,
      maxCfRetries = config.maxCfRetries,
      maxConnectionRetries = config.maxConnectionRetries,
      failureWindowSize = config.failureWindowSize,
      failureThreshold = config.failureThreshold,
      minSampleSize = config.minSampleSize
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
        ZIO.whenDiscard(cumulative.requests > 0)(logger.info(cumulative.summary))
    }

  private def userAgentHeaders(contactEmail: String): Headers =
    Headers(
      Header.Custom("User-Agent", s"${BuildInfo.name.toUpperCase}/${BuildInfo.version} (contact: $contactEmail)"),
      Header.Accept(MediaType.application.json)
    )

  def live(appLabel: String): RLayer[Client & PostgresClient & CcasLogger, ChessComClient] =
    ZLayer.scoped {
      val typesafeConfig = ConfigFactory.load().getConfig("chess-com-client")
      import scala.jdk.CollectionConverters.*
      val tiers          = typesafeConfig.getIntList("recovery-tiers").asScala.map(_.intValue).toVector
      val cooldown       = typesafeConfig.getLong("cooldown-seconds").seconds
      val cfCooldown     = typesafeConfig.getLong("cf-cooldown-seconds").seconds
      val windowSize     = typesafeConfig.getInt("failure-window-size")
      val threshold      = typesafeConfig.getDouble("failure-threshold")
      val minSample      = typesafeConfig.getInt("min-sample-size")
      val retryBase      = typesafeConfig.getLong("retry-base-seconds").seconds
      val cfDelay        = typesafeConfig.getLong("cf-retry-delay-seconds").seconds
      val connRetryBase  = typesafeConfig.getLong("connection-retry-base-seconds").seconds
      val max429         = typesafeConfig.getInt("max-429-retries")
      val maxCf          = typesafeConfig.getInt("max-cf-retries")
      val maxConn        = typesafeConfig.getInt("max-connection-retries")
      val statsFlushInterval = typesafeConfig.getLong("stats-flush-interval-seconds").seconds
      require(
        !statsFlushInterval.isNegative && !statsFlushInterval.isZero,
        s"stats-flush-interval-seconds must be positive, got: $statsFlushInterval"
      )
      val throttleConfig = ThrottleConfig(
        tiers, cooldown, cfCooldown, retryBase, cfDelay, connRetryBase,
        max429, maxCf, maxConn, windowSize, threshold, minSample
      )
      for {
        contactEmail  <- ZIO.attempt(typesafeConfig.getString("contact-email"))
        clientScope   <- ZIO.service[Scope]
        client        <- ZIO.service[Client]
        pgClient      <- ZIO.service[PostgresClient]
        logger        <- ZIO.service[CcasLogger]
        stateRef      <- Ref.make(ThrottleState(throttleConfig.maxPermits, 0, Vector.empty))
        activeRef     <- Ref.make(0)
        rateLimitGate <- Semaphore.make(1)
        lastReqRef    <- Ref.make(0L)
        ema           <- Ref.make(0.0)
        startedAt     <- Clock.instant
        sessionId      = startedAt.toString.replace(":", "").replace("-", "")
        stats         <- Ref.make(StatsAccumulator())
        configIdRef   <- Ref.make(Option.empty[Long])
        bar           <- logger.progressBar
        refs = ThrottleRefs(stateRef, activeRef, rateLimitGate, lastReqRef, ema)
        flushCtx = FlushContext(sessionId, appLabel, startedAt, stats, configIdRef, throttleConfig, stateRef, pgClient)
        flushFiber <- persistStats(flushCtx).repeat(Schedule.fixed(statsFlushInterval)).forkDaemon
        _ <- ZIO.addFinalizer(flushFiber.interrupt *> finalFlush(flushCtx, logger))
      } yield ChessComClient(
        client,
        pgClient,
        userAgentHeaders(contactEmail),
        logger,
        refs,
        stats,
        bar,
        throttleConfig,
        clientScope
      )
    }
}
