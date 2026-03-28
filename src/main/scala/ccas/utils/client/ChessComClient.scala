package ccas.utils.client

import ccas.analysis.tables.ApiFetchFailure
import ccas.info.BuildInfo
import ccas.utils.json.JsonDecodingException
import ccas.utils.{CcasLogger, ProgressBar}
import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import zio.*
import zio.http.*
import zio.http.Method.GET
import zio.json.JsonDecoder

import java.time.Instant

final class ChessComClient(
  client: Client,
  transactor: Transactor,
  headers: Headers,
  logger: CcasLogger,
  semaphore: Semaphore,
  stateRef: Ref[ChessComClient.ThrottleState],
  reserveFibersRef: Ref[Chunk[Fiber.Runtime[Nothing, Nothing]]],
  adjustMutex: Semaphore,
  activeRef: Ref[Int],
  rateLimitGate: Semaphore,
  lastRequestRef: Ref[Long],
  responseTimeEma: Ref[Double],
  progressBar: ProgressBar,
  config: ChessComClient.ThrottleConfig
) {
  private val batchedClient = client.batched @@ ZClientAspect.followRedirects(3) { (_, message) =>
    ZIO.fail(Exception(s"Redirect failed: $message"))
  }

  private def rawGet[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] = for {
    response    <- batchedClient(Request(method = GET, url = url).addHeaders(headers))
    string      <- response.body.asString
    cfChallenge  = isCloudflareChallenge(response, string)
    _           <- if (cfChallenge) throttleDown(_ => 1) else recordOutcome(response.status != Status.TooManyRequests)
    _ <- ZIO.whenDiscard(!response.status.isSuccess)(
      ZIO.fail(HttpStatusException(response.status.code, url, string))
    )
    value <- ZIO.fromEither(jsonDecoder.decodeJson(string)).mapError(JsonDecodingException(_))
  } yield value

  def get[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] =
    (rateDelay *> semaphore.withPermit {
      (activeRef.updateAndGet(_ + 1).flatMap(updateBar) *>
        rawGet(url).timed.flatMap { case (duration, result) =>
          updateResponseTimeEma(duration.toMillis).as(result)
        }).ensuring(activeRef.updateAndGet(_ - 1).flatMap(updateBar).ignore)
    }).retry(retry429Schedule).retry(retryCfSchedule).retry(retryOnceSchedule).tapError { error =>
      val (msg, body) = error match {
        case e: HttpStatusException => (Some(e.statusCode.toString), Some(e.responseBody))
        case other                  => (Option(other.getMessage), None)
      }
      ApiFetchFailure
        .insert(ApiFetchFailure(Instant.now(), url.encode, error.getClass.getSimpleName, msg, body))
        .provideEnvironment(ZEnvironment(transactor))
        .ignore
    }

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

  /** Adaptive inter-request delay derived from an EMA of successful response times.
    * Targets full permit utilization: delay = ema / currentMax. When throttled to fewer
    * permits the delay increases proportionally, naturally slowing the request rate.
    * Skipped during cold start (EMA = 0) where the semaphore alone governs concurrency.
    * Uses a single-permit gate to serialize entry so concurrent callers stagger correctly.
    * Runs outside `semaphore.withPermit`, so no concurrency permit is held during sleep.
    */
  private def rateDelay: Task[Unit] =
    responseTimeEma.get.flatMap { ema =>
      ZIO.unlessDiscard(ema <= 0) {
        rateLimitGate.withPermit {
          stateRef.get.flatMap { state =>
            val targetDelay = (ema / state.currentMax).toLong
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
    }

  private val emaAlpha = 0.1

  private def updateResponseTimeEma(responseMs: Long): UIO[Unit] =
    responseTimeEma.update { ema =>
      if (ema <= 0) responseMs.toDouble
      else emaAlpha * responseMs + (1 - emaAlpha) * ema
    }

  /** Record a request outcome and trigger throttle-down if failure rate exceeds threshold.
    * Skipped while cooling down (after a recent throttle-down) to prevent cascading reductions
    * from a single burst of 429s.
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

  /** Reduce the effective permit limit by the given divisor and schedule recovery.
    * Clears the outcome window and sets coolingDown to prevent cascading reductions.
    */
  private def throttleDown(compute: Long => Long = _ / 2): Task[Unit] =
    stateRef.modify { state =>
      if (state.coolingDown) {
        (None, state)
      } else {
        val newMax = compute(state.currentMax).max(1)
        if (newMax == state.currentMax) {
          (None, state)
        } else {
        val newGen = state.generation + 1
        val newState = state.copy(
          currentMax = newMax,
          generation = newGen,
          outcomes = Vector.empty,
          coolingDown = true
        )
        (Some((state.currentMax, newMax, newGen)), newState)
        }
      }
    }.flatMap {
      case None => ZIO.unit
      case Some((oldMax, newMax, gen)) =>
        adjustPermits(newMax) *>
          logger.warn(s"Rate limit throttle: $oldMax \u2192 $newMax permits") *>
          scheduleRecovery(gen).forkDaemon.unit
    }

  /** Incrementally adjust reserve fibers to enforce `newMax` effective permits.
    * Throttle-down only forks additional reserve fibers (never releasing existing ones),
    * so no permits are briefly returned to the semaphore for queued requests to grab.
    * Recovery only interrupts the excess. Serialized via mutex.
    */
  private def adjustPermits(newMax: Long): Task[Unit] =
    adjustMutex.withPermit {
      for {
        oldFibers <- reserveFibersRef.get
        toReserve  = (config.maxPermits - newMax).toInt
        current    = oldFibers.size
        delta      = toReserve - current
        _ <- if (delta > 0) {
          ZIO.foreach(Chunk.range(0, delta))(_ =>
            semaphore.withPermit(ZIO.never).forkDaemon
          ).flatMap(added => reserveFibersRef.set(oldFibers ++ added))
        } else {
          ZIO.whenDiscard(delta < 0) {
            val (keep, release) = oldFibers.splitAt(current + delta)
            ZIO.foreachDiscard(release)(_.interrupt) *> reserveFibersRef.set(keep)
          }
        }
      } yield ()
    }

  /** Sleep for cooldown, then recover permits if failure rate has dropped.
    * Clears coolingDown so further throttle-downs can occur if needed.
    */
  private def scheduleRecovery(generation: Long): Task[Unit] =
    for {
      _ <- ZIO.sleep(config.cooldown)
      option <- stateRef.modify { state =>
        if (state.generation != generation) (None, state)
        else if (failureRate(state.outcomes) > config.failureThreshold) {
          (Some((state.currentMax, state.currentMax, generation)), state.copy(coolingDown = false))
        }
        else {
          val newMax = (state.currentMax * 2).min(config.maxPermits)
          val clearOutcomes = newMax == config.maxPermits
          val newState = state.copy(
            currentMax = newMax,
            coolingDown = false,
            outcomes = if (clearOutcomes) Vector.empty else state.outcomes
          )
          (Some((state.currentMax, newMax, generation)), newState)
        }
      }
      _ <- ZIO.foreachDiscard(option) { case (oldMax, newMax, gen) =>
        if (oldMax == newMax) {
          logger.warn(s"Rate limit still elevated, holding at $newMax permits") *> scheduleRecovery(gen)
        }
        else {
          val msg =
            if (newMax == config.maxPermits) "Rate limit throttle lifted"
            else s"Rate limit easing: $oldMax \u2192 $newMax permits"
          adjustPermits(newMax) *> logger.info(msg) *>
            ZIO.unlessDiscard(newMax == config.maxPermits)(scheduleRecovery(gen))
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
      case _ => false
    }

  private val retryOnceSchedule: Schedule[Any, Throwable, Any] =
    Schedule.fixed(config.singleRetryDelay) && Schedule.recurs(1) && Schedule.recurWhile[Throwable] {
      case e: HttpStatusException =>
        (e.statusCode == 403 && !e.responseBody.contains(CfChallengeMarker)) || e.statusCode == 404
      case _ => false
    }
}

object ChessComClient {
  private[ccas] case class ThrottleState(
    currentMax: Long,
    generation: Long,
    outcomes: Vector[Boolean],
    coolingDown: Boolean = false
  )

  /** @param maxPermits       Maximum concurrent requests (semaphore permits). Throttle-down halves from here; recovery doubles back.
    * @param cooldown         Time to wait after a throttle-down before attempting recovery.
    * @param retryBase        Base duration for exponential-backoff retry on 429 responses.
    * @param singleRetryDelay Fixed delay for a single retry on 403/404 responses.
    * @param cfRetryDelay     Fixed delay between retries on Cloudflare challenge 403 responses.
    * @param failureWindowSize Rolling window size for tracking success/failure outcomes.
    * @param failureThreshold Fraction of failures in the window (0.0–1.0) that triggers a throttle-down.
    * @param minSampleSize    Minimum outcomes in the window before the failure rate is evaluated.
    */
  private[ccas] case class ThrottleConfig(
    maxPermits: Long,
    cooldown: Duration,
    retryBase: Duration,
    singleRetryDelay: Duration,
    cfRetryDelay: Duration,
    failureWindowSize: Int,
    failureThreshold: Double,
    minSampleSize: Int
  )

  private def userAgentHeaders(contactEmail: String): Headers =
    Headers(
      Header.Custom("User-Agent", s"${BuildInfo.name.toUpperCase}/${BuildInfo.version} (contact: $contactEmail)"),
      Header.Accept(MediaType.application.json),
    )

  def live: ZLayer[Client & Transactor & CcasLogger, Throwable, ChessComClient] =
    ZLayer.scoped {
      val typesafeConfig = ConfigFactory.load().getConfig("chess-com-client")
      val permits        = typesafeConfig.getLong("permits")
      val cooldown       = typesafeConfig.getLong("cooldown-seconds").seconds
      val windowSize     = typesafeConfig.getInt("failure-window-size")
      val threshold      = typesafeConfig.getDouble("failure-threshold")
      val minSample      = typesafeConfig.getInt("min-sample-size")
      val singleDelay    = typesafeConfig.getLong("single-retry-delay-seconds").seconds
      val cfDelay        = typesafeConfig.getLong("cf-retry-delay-seconds").seconds
      val throttleConfig = ThrottleConfig(permits, cooldown, 1.second, singleDelay, cfDelay, windowSize, threshold, minSample)
      for {
        contactEmail <- ZIO.attempt(typesafeConfig.getString("contact-email"))
        client       <- ZIO.service[Client]
        transactor   <- ZIO.service[Transactor]
        logger       <- ZIO.service[CcasLogger]
        semaphore    <- Semaphore.make(permits)
        stateRef     <- Ref.make(ThrottleState(permits, 0, Vector.empty))
        reserveRef   <- Ref.make(Chunk.empty[Fiber.Runtime[Nothing, Nothing]])
        adjustMutex  <- Semaphore.make(1)
        activeRef    <- Ref.make(0)
        rateLimitGate <- Semaphore.make(1)
        lastReqRef   <- Ref.make(0L)
        ema          <- Ref.make(0.0)
        bar          <- logger.progressBar
        _            <- ZIO.addFinalizer(reserveRef.get.flatMap(fibers => ZIO.foreachDiscard(fibers)(_.interrupt)))
      } yield ChessComClient(
        client, transactor, userAgentHeaders(contactEmail), logger,
        semaphore, stateRef, reserveRef, adjustMutex, activeRef, rateLimitGate, lastReqRef, ema, bar, throttleConfig
      )
    }
}
