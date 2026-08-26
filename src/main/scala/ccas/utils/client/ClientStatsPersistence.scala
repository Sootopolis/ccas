package ccas.utils.client

import java.time.Instant

import ccas.analysis.tables.{ClientConfig, ClientStats}
import ccas.utils.errors.safeMessage
import ccas.utils.sql.PostgresClient
import zio.{Clock, Fiber, Ref, UIO, ZEnvironment, ZIO}

/** Bundles the refs and config needed by periodic and final stats flushes. Created once in `ChessComClient.live` and
  * passed to `persistStats` / `finalFlush`.
  *
  * `endedAtRef` pins the session-window end. While `None` (the normal periodic case) each flush stamps `completedAt`
  * with the live clock; once `endSession` sets it, every subsequent flush — including the scope-close `finalFlush` —
  * reuses that instant, so a human pause after the last API request (e.g. a CLI confirmation prompt) does not stretch
  * the persisted window or deflate throughput.
  */
private[ccas] case class ClientStatsFlushContext(
  sessionId: String,
  appLabel: String,
  startedAt: Instant,
  statsRef: Ref[ClientStatsAccumulator],
  configIdRef: Ref[Option[Long]],
  config: ChessComClient.ThrottleConfig,
  stateRef: Ref[ChessComClient.ThrottleState],
  pgClient: PostgresClient,
  endedAtRef: Ref[Option[Instant]]
)

private[ccas] object ClientStatsPersistence {

  /** Upsert the cumulative stats snapshot for this session. Each call overwrites the single row for this session,
    * creating it on first flush. On first insert, also inserts the `ClientConfig` row. Silently swallows DB errors
    * so it is safe to call from finalizers and background fibers.
    */
  def persistStats(ctx: ClientStatsFlushContext): UIO[Unit] =
    (for {
      current <- ctx.statsRef.get
      _ <- ZIO.whenDiscard(current.hasActivity) {
        for {
          now        <- ctx.endedAtRef.get.flatMap(_.fold(Clock.instant)(ZIO.succeed(_)))
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
    } yield ())
      .tapError(e => ZIO.logWarning(s"Failed to persist client_stats for session ${ctx.sessionId}: ${e.safeMessage}"))
      .ignore

  /** Marks the end of API work: pins the window-end, stops the periodic flush fiber, and performs one final pinned
    * flush. Call it after the last request but before any human pause, so the persisted window reflects API work
    * rather than wall-clock-until-exit. Idempotent — pinning takes only on the first call.
    *
    * The flush is immediate, not just the pin, so the pinned snapshot survives a kill during that pause.
    * Interruption is forked rather than awaited, since a flush stuck on a dead DB socket would block the caller for
    * `socketTimeout`; the resulting overlap is harmless because both writers upsert the same row and swallow errors.
    */
  def endSession(ctx: ClientStatsFlushContext, flushFiber: Fiber[Any, Any]): UIO[Unit] =
    for {
      now <- Clock.instant
      _   <- ctx.endedAtRef.update(_.orElse(Some(now)))
      _   <- flushFiber.interruptFork
      _   <- persistStats(ctx)
    } yield ()

  /** Final stats flush: upserts the cumulative snapshot, then logs a summary. Called by the scope finalizer. */
  def finalFlush(ctx: ClientStatsFlushContext): UIO[Unit] =
    ctx.statsRef.get.flatMap { cumulative =>
      persistStats(ctx) *>
        ZIO.whenDiscard(cumulative.hasActivity)(ZIO.logInfo(cumulative.summary))
    }

  private def toClientConfig(config: ChessComClient.ThrottleConfig): ClientConfig = {
    val cc = ClientConfig(
      configId = 0,
      configHash = "",
      recoveryTiers = config.recoveryTiers.toList,
      minRequestDelayMs = config.minRequestDelayMs,
      emaTauMs = config.emaTauMs,
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

  private def inProgressThrottleMs(stateRef: Ref[ChessComClient.ThrottleState]): UIO[Long] =
    Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS).flatMap { now =>
      stateRef.get.map(_.throttledSince.fold(0L)(now - _))
    }
}
