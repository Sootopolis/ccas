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
  *   - '''Failure-window throttle-down''' — maintains a rolling window of success/failure outcomes. Rate-limiting
  *     signals (429 and non-Cloudflare 403) count as failures; other errors (404, 500) do not. Cloudflare challenge
  *     403s bypass the window and trigger an immediate hard throttle. When the failure rate in the window exceeds a
  *     configurable threshold, `currentMax` drops to 1 and the gate immediately enforces it.
  *   - '''Generation-gated recovery''' — after a cooldown period, a background fiber doubles the permit limit back (or
  *     holds if failures persist). A generation counter ensures that only the most recent throttle-down triggers
  *     recovery, preventing stale fibers from interfering. Recovery fibers are scoped to the client's lifetime and
  *     interrupted when the layer is torn down.
  *   - '''Retry schedules''' — separate schedules handle HTTP 429 (exponential backoff), Cloudflare challenge 403s
  *     (fixed delay), normal 403 (single retry), and transient connection errors (exponential backoff). HTTP 404s are
  *     not retried as they are almost always permanent (e.g. cancelled matches on Chess.com).
  *   - '''Periodic stats flushing''' — a background daemon fiber persists accumulated request statistics to the
  *     `client_stats` table at a configurable interval (`stats-flush-interval-seconds`). On first flush, also inserts
  *     the throttle configuration into `client_config` and links the stats row via FK. Subsequent flushes UPDATE the
  *     same stats row in place. Stats include gate wait time, EMA delay time, and total time spent at reduced permits
  *     (`throttled_ms`), plus a `secs_per_request` goodness indicator. A final flush runs in the scope finalizer.
  *     This ensures stats survive non-graceful shutdowns with at most one interval's worth of data loss.
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
    _ <- statsRef.update(_.incAttempts)
    response <- batchedClient(Request(method = GET, url = url).addHeaders(headers)).tapError { e =>
      ZIO.whenDiscard(isConnectionError(e))(statsRef.update(_.incConnectionErrors))
    }
    string <- response.body.asString
    cfChallenge = isCloudflareChallenge(response, string)
    _ <-
      if (cfChallenge) throttleDown(config.cfCooldown)
      else recordOutcome(response.status != Status.TooManyRequests && response.status != Status.Forbidden)
    errorBody = if (cfChallenge) ApiResponseBody.CfCanonicalBody else string
    _ <- ZIO.whenDiscard(!response.status.isSuccess) {
      statsRef.update(_.incError(response.status.code)) *>
        ZIO.fail(HttpStatusException(response.status.code, url, errorBody))
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
        _ <- updateResponseTimeEma(ms) *> statsRef.update(_.recordLatency(ms))
      } yield result
    }

  private def withRetries[T](effect: Task[T]): Task[T] =
    effect
      .retry(retry429Schedule)
      .retry(retryCfSchedule)
      .retry(retry403Schedule)
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

  /** Wait for capacity inside the serializing gate and apply the EMA-based inter-request delay. The gate is released
    * before the request starts so that admission is interruptible — only the fast `activeRef` increment that follows is
    * uninterruptible (via `acquireReleaseWith` in `get`).
    */

  private def releaseSlot(): UIO[Unit] =
    activeRef.updateAndGet(_ - 1).flatMap(updateBar).ignore

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
    }.flatMap { shouldThrottle =>
      ZIO.whenDiscard(shouldThrottle)(throttleDown())
    }

  /** Drop the effective concurrency limit to 1 and schedule drain-then-recover. Clears the outcome window and sets
    * coolingDown to prevent cascading reductions. The `cooldown` parameter controls how long to wait after in-flight
    * requests have drained before attempting recovery.
    */
  private def throttleDown(cooldown: Duration = config.cooldown): Task[Unit] =
    Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap { now =>
      stateRef.modify { state =>
        if (state.coolingDown || state.currentMax <= 1) {
          (None, state)
        } else {
          val newGen = state.generation + 1
          val newState = state.copy(
            currentMax = 1,
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
        statsRef.update(_.incThrottleDowns) *>
          logger.warn(s"Rate limit throttle: $oldMax \u2192 1 permit") *>
          scheduleRecovery(gen, cooldown).forkDaemon
            .flatMap(f => scope.addFinalizerExit(_ => f.interrupt)).unit
    }

  /** Wait for in-flight requests to drain, sleep for cooldown, then recover permits if failure rate has dropped. Clears
    * coolingDown so further throttle-downs can occur if needed.
    */
  private def scheduleRecovery(generation: Long, cooldown: Duration): Task[Unit] =
    for {
      _ <- activeRef.get.repeat(Schedule.spaced(200.millis) && Schedule.recurWhile(_ > 1)).unit
      _ <- ZIO.sleep(cooldown)
      now <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
      option <- stateRef.modify { state =>
        if (state.generation != generation) (None, state)
        else if (failureRate(state.outcomes) > config.failureThreshold) {
          (Some((state.currentMax, state.currentMax, generation, 0L)), state.copy(coolingDown = false))
        } else {
          val newMax        = (state.currentMax * 2).min(config.maxPermits)
          val clearOutcomes = newMax == config.maxPermits
          val throttleDuration =
            if (clearOutcomes) state.throttledSince.map(now - _).getOrElse(0L)
            else 0L
          val newState = state.copy(
            currentMax = newMax,
            coolingDown = false,
            outcomes = if (clearOutcomes) Vector.empty else state.outcomes,
            throttledSince = if (clearOutcomes) None else state.throttledSince
          )
          (Some((state.currentMax, newMax, generation, throttleDuration)), newState)
        }
      }
      _ <- ZIO.foreachDiscard(option) { case (oldMax, newMax, gen, throttleDuration) =>
        ZIO.whenDiscard(throttleDuration > 0)(statsRef.update(_.addThrottled(throttleDuration))) *> {
          if (oldMax == newMax) {
            logger.warn(s"Rate limit still elevated, holding at $newMax permits") *> scheduleRecovery(gen, cooldown)
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
    Schedule.exponential(config.retryBase).jittered && Schedule.recurs(5) && Schedule.recurWhile[Throwable] {
      case e: HttpStatusException => e.statusCode == 429
      case _                      => false
    }

  private val retryCfSchedule: Schedule[Any, Throwable, Any] =
    Schedule.fixed(config.cfRetryDelay) && Schedule.recurs(2) && Schedule.recurWhile[Throwable] {
      case e: HttpStatusException => e.statusCode == 403 && e.responseBody.contains(CfChallengeMarker)
      case _                      => false
    }

  private val retry403Schedule: Schedule[Any, Throwable, Any] =
    Schedule.fixed(config.singleRetryDelay) && Schedule.recurs(1) && Schedule.recurWhile[Throwable] {
      case e: HttpStatusException => e.statusCode == 403 && !e.responseBody.contains(CfChallengeMarker)
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
    Schedule.exponential(500.millis) && Schedule.recurs(3) && Schedule.recurWhile[Throwable](isConnectionError)
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

  /** @param maxPermits
    *   Maximum concurrent in-flight requests. Throttle-down drops to 1; recovery doubles back.
    * @param cooldown
    *   Time to wait after draining in-flight requests before attempting recovery from a failure-rate throttle-down.
    * @param cfCooldown
    *   Time to wait after draining in-flight requests before attempting recovery from a Cloudflare throttle-down.
    * @param retryBase
    *   Base duration for exponential-backoff retry on 429 responses.
    * @param singleRetryDelay
    *   Fixed delay for a single retry on non-Cloudflare 403 responses.
    * @param cfRetryDelay
    *   Fixed delay between retries on Cloudflare challenge 403 responses.
    * @param failureWindowSize
    *   Rolling window size for tracking success/failure outcomes.
    * @param failureThreshold
    *   Fraction of failures in the window (0.0–1.0) that triggers a throttle-down.
    * @param minSampleSize
    *   Minimum outcomes in the window before the failure rate is evaluated.
    */
  private[ccas] case class ThrottleConfig(
    maxPermits: Long,
    cooldown: Duration,
    cfCooldown: Duration,
    retryBase: Duration,
    singleRetryDelay: Duration,
    cfRetryDelay: Duration,
    failureWindowSize: Int,
    failureThreshold: Double,
    minSampleSize: Int
  )

  private[ccas] case class StatsAccumulator(
    requests: Long = 0,
    successes: Long = 0,
    failures: Long = 0,
    attempts: Long = 0,
    errors429: Long = 0,
    errors403: Long = 0,
    errors404: Long = 0,
    connectionErrors: Long = 0,
    throttleDowns: Long = 0,
    peakConcurrent: Int = 0,
    latencyMinMs: Long = Long.MaxValue,
    latencyMaxMs: Long = 0,
    latencySumMs: Long = 0,
    latencyCount: Long = 0,
    gateWaitMs: Long = 0,
    emaDelayMs: Long = 0,
    throttledMs: Long = 0
  ) {
    def incRequests: StatsAccumulator         = copy(requests = requests + 1)
    def incSuccesses: StatsAccumulator        = copy(successes = successes + 1)
    def incFailures: StatsAccumulator         = copy(failures = failures + 1)
    def incAttempts: StatsAccumulator         = copy(attempts = attempts + 1)
    def incConnectionErrors: StatsAccumulator = copy(connectionErrors = connectionErrors + 1)
    def incThrottleDowns: StatsAccumulator    = copy(throttleDowns = throttleDowns + 1)
    def updatePeak(n: Int): StatsAccumulator  = copy(peakConcurrent = peakConcurrent.max(n))
    def addGateWait(ms: Long): StatsAccumulator = copy(gateWaitMs = gateWaitMs + ms)
    def addEmaDelay(ms: Long): StatsAccumulator = copy(emaDelayMs = emaDelayMs + ms)
    def addThrottled(ms: Long): StatsAccumulator = copy(throttledMs = throttledMs + ms)

    def incError(statusCode: Int): StatsAccumulator = statusCode match {
      case 429 => copy(errors429 = errors429 + 1)
      case 403 => copy(errors403 = errors403 + 1)
      case 404 => copy(errors404 = errors404 + 1)
      case _   => this
    }

    def recordLatency(ms: Long): StatsAccumulator = copy(
      latencyMinMs = latencyMinMs.min(ms),
      latencyMaxMs = latencyMaxMs.max(ms),
      latencySumMs = latencySumMs + ms,
      latencyCount = latencyCount + 1
    )

    def toClientStats(
      appLabel: String,
      startedAt: Instant,
      completedAt: Instant,
      configId: Long
    ): ClientStats = {
      val minDisplay      = if (latencyMinMs == Long.MaxValue) 0L else latencyMinMs
      val meanLatency     = if (latencyCount > 0) latencySumMs / latencyCount else 0L
      val wallClockSecs   = java.time.Duration.between(startedAt, completedAt).toMillis / 1000.0
      val requestsPerSec  = if (wallClockSecs > 0) successes.toDouble / wallClockSecs else 0.0
      ClientStats(
        appLabel = appLabel,
        startedAt = startedAt,
        completedAt = completedAt,
        configId = configId,
        requestsPerSec = requestsPerSec,
        requests = requests,
        successes = successes,
        failures = failures,
        attempts = attempts,
        errors429 = errors429,
        errors403 = errors403,
        errors404 = errors404,
        connectionErrors = connectionErrors,
        throttleDowns = throttleDowns,
        throttledMs = throttledMs,
        peakConcurrent = peakConcurrent,
        gateWaitMs = gateWaitMs,
        emaDelayMs = emaDelayMs,
        latencyMinMs = minDisplay,
        latencyMaxMs = latencyMaxMs,
        latencyMeanMs = meanLatency
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
    * `persistStats` / `finalFlush` so they don't each need eight parameters.
    */
  private[ccas] case class FlushContext(
    appLabel: String,
    startedAt: Instant,
    statsRef: Ref[StatsAccumulator],
    statsRowId: Ref[Option[Long]],
    configIdRef: Ref[Option[Long]],
    config: ThrottleConfig,
    stateRef: Ref[ThrottleState],
    pgClient: PostgresClient
  )

  /** Persist a snapshot of accumulated stats to the database. Inserts a new row on the first call with `requests > 0`
    * and updates the same row on subsequent calls. On first insert, also inserts the `ClientConfig` row. Silently
    * swallows DB errors so it is safe to call from finalizers and background fibers.
    */
  private[ccas] def persistStats(ctx: FlushContext): UIO[Unit] =
    (for {
      s <- ctx.statsRef.get
      _ <- ZIO.whenDiscard(s.requests > 0) {
        for {
          now        <- ZIO.succeed(Instant.now())
          inProgress <- inProgressThrottleMs(ctx.stateRef)
          adjusted    = s.addThrottled(inProgress)
          configId   <- ctx.configIdRef.get.flatMap {
            case Some(id) => ZIO.succeed(id)
            case None =>
              ClientConfig.insert(toClientConfig(ctx.config)).provideEnvironment(ZEnvironment(ctx.pgClient))
                .tap(id => ctx.configIdRef.set(Some(id)))
          }
          row = adjusted.toClientStats(ctx.appLabel, ctx.startedAt, now, configId)
          _ <- ctx.statsRowId.get.flatMap {
            case Some(id) =>
              ClientStats.updateById(id, row).provideEnvironment(ZEnvironment(ctx.pgClient)).unit
            case None =>
              ClientStats.insertReturningId(row).provideEnvironment(ZEnvironment(ctx.pgClient))
                .flatMap(id => ctx.statsRowId.set(Some(id)))
          }
        } yield ()
      }
    } yield ()).ignore

  private def toClientConfig(config: ThrottleConfig): ClientConfig =
    ClientConfig(
      configId = 0,
      permits = config.maxPermits.toInt,
      cooldownSecs = config.cooldown.getSeconds.toInt,
      cfCooldownSecs = config.cfCooldown.getSeconds.toInt,
      retryBaseSecs = config.retryBase.getSeconds.toInt,
      singleRetrySecs = config.singleRetryDelay.getSeconds.toInt,
      cfRetrySecs = config.cfRetryDelay.getSeconds.toInt,
      failureWindowSize = config.failureWindowSize,
      failureThreshold = config.failureThreshold,
      minSampleSize = config.minSampleSize
    )

  private def inProgressThrottleMs(stateRef: Ref[ThrottleState]): UIO[Long] =
    Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap { now =>
      stateRef.get.map(_.throttledSince.map(now - _).getOrElse(0L))
    }

  /** Final stats flush: persists the latest snapshot and logs a human-readable summary. Called by the scope finalizer. */
  private def finalFlush(ctx: FlushContext, logger: CcasLogger): UIO[Unit] =
    persistStats(ctx) *>
      ctx.statsRef.get.flatMap { s =>
        ZIO.whenDiscard(s.requests > 0)(logger.info(s.summary))
      }

  private def userAgentHeaders(contactEmail: String): Headers =
    Headers(
      Header.Custom("User-Agent", s"${BuildInfo.name.toUpperCase}/${BuildInfo.version} (contact: $contactEmail)"),
      Header.Accept(MediaType.application.json)
    )

  def live(appLabel: String): RLayer[Client & PostgresClient & CcasLogger, ChessComClient] =
    ZLayer.scoped {
      val typesafeConfig = ConfigFactory.load().getConfig("chess-com-client")
      val permits        = typesafeConfig.getLong("permits")
      val cooldown       = typesafeConfig.getLong("cooldown-seconds").seconds
      val cfCooldown     = typesafeConfig.getLong("cf-cooldown-seconds").seconds
      val windowSize     = typesafeConfig.getInt("failure-window-size")
      val threshold      = typesafeConfig.getDouble("failure-threshold")
      val minSample      = typesafeConfig.getInt("min-sample-size")
      val singleDelay    = typesafeConfig.getLong("single-retry-delay-seconds").seconds
      val cfDelay        = typesafeConfig.getLong("cf-retry-delay-seconds").seconds
      val statsFlushInterval = typesafeConfig.getLong("stats-flush-interval-seconds").seconds
      val throttleConfig =
        ThrottleConfig(permits, cooldown, cfCooldown, 1.second, singleDelay, cfDelay, windowSize, threshold, minSample)
      for {
        contactEmail  <- ZIO.attempt(typesafeConfig.getString("contact-email"))
        clientScope   <- ZIO.service[Scope]
        client        <- ZIO.service[Client]
        pgClient      <- ZIO.service[PostgresClient]
        logger        <- ZIO.service[CcasLogger]
        stateRef      <- Ref.make(ThrottleState(permits, 0, Vector.empty))
        activeRef     <- Ref.make(0)
        rateLimitGate <- Semaphore.make(1)
        lastReqRef    <- Ref.make(0L)
        ema           <- Ref.make(0.0)
        startedAt     <- ZIO.succeed(Instant.now())
        stats         <- Ref.make(StatsAccumulator())
        statsRowId    <- Ref.make(Option.empty[Long])
        configIdRef   <- Ref.make(Option.empty[Long])
        bar           <- logger.progressBar
        refs = ThrottleRefs(stateRef, activeRef, rateLimitGate, lastReqRef, ema)
        flushCtx = FlushContext(appLabel, startedAt, stats, statsRowId, configIdRef, throttleConfig, stateRef, pgClient)
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
