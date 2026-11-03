package ccas.utils.client

import java.time.Instant
import java.util.concurrent.TimeUnit

import zio.{Clock, Fiber, Ref, UIO, ZEnvironment, ZIO}

import ccas.analysis.tables.{ClientConfig, ClientStats}
import ccas.utils.errors.safeMessage
import ccas.utils.sql.PostgresClient

/** Bundles the refs and config needed by periodic and final stats flushes. Created once in `ChessComClient.live` and
  * passed to `persistStats` / `finalFlush`.
  *
  * `endedAtRef` pins the session-window end. While `None` (the normal periodic case) each write stamps `completedAt`
  * with the live clock; once `endSession` sets it, every subsequent write — including one by the scope-close
  * `finalFlush` — reuses that instant, so a human pause after the last API request (e.g. a CLI confirmation prompt)
  * does not stretch the persisted window or deflate throughput.
  *
  * `lastFlushedRef` holds the totals of the last successful write, empty before the first, so an idle session can tell
  * it has nothing to say.
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
  endedAtRef: Ref[Option[Instant]],
  lastFlushedRef: Ref[ClientStatsAccumulator]
)

private[ccas] object ClientStatsPersistence {

  /** Upsert the cumulative stats snapshot for this session, if it has changed since the last successful write. An idle
    * session therefore writes nothing, so it cannot hold a Neon compute awake, and `completed_at` stays at its last
    * write: see `docs/adr/0012-gate-based-adaptive-throttle.md`. Each write overwrites the single row for this session,
    * creating it on first flush. On first insert, also inserts the `ClientConfig` row. Silently swallows DB errors
    * so it is safe to call from finalisers and background fibers.
    */
  def persistStats(ctx: ClientStatsFlushContext): UIO[Unit] =
    (for {
      current <- ctx.statsRef.get
      flushed <- ctx.lastFlushedRef.get
      _ <- ZIO.whenDiscard(current.hasActivity && current != flushed) {
        for {
          now        <- ctx.endedAtRef.get.flatMap(_.fold(Clock.instant)(ZIO.succeed(_)))
          inProgress <- inProgressThrottleMs(ctx.stateRef)
          configId <- ctx.configIdRef.get.flatMap {
            case Some(id) => ZIO.succeed(id)
            case None =>
              ClientConfig.ensureConfig(ctx.config.toClientConfig)
                .provideEnvironment(ZEnvironment(ctx.pgClient))
                .tap(id => ctx.configIdRef.set(Some(id)))
          }
          row = current.toClientStats(ctx.sessionId, ctx.appLabel, ctx.startedAt, now, configId, inProgress)
          _ <- ClientStats.upsert(row).provideEnvironment(ZEnvironment(ctx.pgClient))
          _ <- ctx.lastFlushedRef.set(current)
        } yield ()
      }
    } yield ())
      .tapError(e => ZIO.logWarning(s"Failed to persist client_stats for session ${ctx.sessionId}: ${e.safeMessage}"))
      .ignore

  /** Marks the end of API work: pins the window-end, stops the periodic flush fiber, and writes any totals not yet in
    * the row under the pin. Call it after the last request but before any human pause, so the persisted window reflects
    * API work rather than wall-clock-until-exit. Idempotent — pinning takes only on the first call.
    *
    * The flush is immediate, not just the pin, so the pinned snapshot survives a kill during that pause. With nothing
    * new to write, the row keeps its last write's earlier `completed_at`. Interruption is forked rather than awaited,
    * since a flush stuck on a dead DB socket would block the caller for `socketTimeout`. Both writers upsert the same
    * row and swallow errors, so the worst case is a stalled periodic write landing last, which leaves its totals there.
    */
  def endSession(ctx: ClientStatsFlushContext, flushFiber: Fiber[Any, Any]): UIO[Unit] =
    for {
      now <- Clock.instant
      _   <- ctx.endedAtRef.update(_.orElse(Some(now)))
      _   <- flushFiber.interruptFork
      _   <- persistStats(ctx)
    } yield ()

  /** Final stats flush: one last [[persistStats]], then logs a summary. Called by the scope finaliser. */
  def finalFlush(ctx: ClientStatsFlushContext): UIO[Unit] =
    ctx.statsRef.get.flatMap { cumulative =>
      persistStats(ctx) *>
        ZIO.whenDiscard(cumulative.hasActivity)(ZIO.logInfo(cumulative.summary))
    }

  private def inProgressThrottleMs(stateRef: Ref[ChessComClient.ThrottleState]): UIO[Long] =
    Clock.currentTime(TimeUnit.MILLISECONDS).flatMap { now =>
      stateRef.get.map(_.throttledSince.fold(0L)(now - _))
    }
}
