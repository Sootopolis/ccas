package ccas.utils.client

import java.time.Instant

import ccas.analysis.tables.{ClientConfig, ClientStats}
import ccas.utils.errors.safeMessage
import ccas.utils.sql.PostgresClient
import zio.{Clock, Ref, UIO, ZEnvironment, ZIO}

/** Bundles the refs and config needed by periodic and final stats flushes. Created once in `ChessComClient.live` and
  * passed to `persistStats` / `finalFlush`.
  */
private[ccas] case class ClientStatsFlushContext(
  sessionId: String,
  appLabel: String,
  startedAt: Instant,
  statsRef: Ref[ClientStatsAccumulator],
  configIdRef: Ref[Option[Long]],
  config: ChessComClient.ThrottleConfig,
  stateRef: Ref[ChessComClient.ThrottleState],
  pgClient: PostgresClient
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
    } yield ())
      .tapError(e => ZIO.logWarning(s"Failed to persist client_stats for session ${ctx.sessionId}: ${e.safeMessage}"))
      .ignore

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
