package ccas.utils

import com.typesafe.config.ConfigFactory

import ccas.utils.client.ChessComClient

object ApiConcurrency {

  /** Hikari `maximumPoolSize` for the default `database` prefix, read once from `ConfigFactory.load()`.
    *
    * Read from config rather than from a live `PostgresClient`, so a client built under a non-default prefix is
    * invisible here and the DB ceiling silently falls away — as does an absent path, which yields `Int.MaxValue`.
    * Both are accepted: `docs/adr/0004-api-fan-out-concurrency-cap.md`.
    */
  private lazy val dbMaxPoolSize: Int = {
    val config = ConfigFactory.load()
    val path   = "database.pool.maximumPoolSize"
    if (config.hasPath(path)) config.getInt(path) else Int.MaxValue
  }

  /** Connections withheld from API fan-out so a DB-phase burst can't check out the whole pool and leave health
    * probes and ad-hoc queries queuing on `connectionTimeout`.
    */
  private[utils] val PoolReserve: Int = 2

  /** `min(2 * maxPermits, dbPool - PoolReserve)`, taking the reserve only when `dbPool > 2 * PoolReserve` so that a
    * small pool isn't strangled — fan-out always keeps at least half the pool.
    *
    * Pure so the rule is testable without `ConfigFactory`; `TestApiConcurrency` holds the worked cases. Why each
    * ceiling exists, and why the guard is worth its non-monotonic boundary:
    * `docs/adr/0004-api-fan-out-concurrency-cap.md` (#91).
    */
  private[utils] def cappedFor(maxPermits: Int, dbPool: Int): Int = {
    val dbCeiling = if (dbPool <= 2 * PoolReserve) dbPool else dbPool - PoolReserve
    // Floor at 1: `withParallelism(0)` is invalid. Unreachable — Hikari rejects pool 0 — so purely defensive.
    math.max(1, math.min(maxPermits * 2, dbCeiling))
  }

  /** Recommended cap on concurrent fibers for API-bound `foreachPar` against the given client. A fiber holds a
    * pooled connection only for the span of each `connectZIO` / `transactZIO`, so fibers may safely exceed
    * connections; the cap is what stops one app self-starving the pool during a DB-phase burst.
    *
    * `maxPermits` is the last `recovery-tiers` entry: the checked-in default `[2, 4, 6, 8]` gives 8, so a default
    * checkout against a pool of 20 caps at 16 (gate-bound). Deployments that raise the tiers to end at 16 cap at 18
    * (pool-bound). Both cases are asserted in `TestApiConcurrency`.
    */
  def fiberCap(client: ChessComClient): Int = cappedFor(client.maxPermits, dbMaxPoolSize)
}
