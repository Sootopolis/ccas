package ccas.utils

import com.typesafe.config.ConfigFactory

import ccas.utils.client.ChessComClient

object ApiConcurrency {

  /** Configured Hikari `maximumPoolSize` for the default `database` pool, read once from `ConfigFactory.load()` on
    * first use. Mirrors the value `PostgresClient.live` applies under its default prefix (`database.pool.maximumPoolSize`,
    * default 20, overridable via `DB_POOL_MAX`) — the same direct-config shortcut `Tables` uses for cache config, rather
    * than threading a `PostgresClient` through every `fiberCap` callsite for what is a static ceiling.
    *
    * This reads config independently of how any `PostgresClient` is actually constructed, with two consequences: (1) it
    * **intentionally** assumes the default `"database"` prefix, so a `PostgresClient.live(prefix = …)` built under
    * another prefix is not seen here and the DB cap falls away (reverting `fiberCap` to `2 * maxPermits`); (2) when the
    * path is absent it yields `Int.MaxValue` (same effect — no DB cap). Every current caller uses the default prefix, so
    * (1) is latent; threading the resolved prefix is the deliberate upgrade trigger only if a second
    * `PostgresClient.live(prefix = …)` is ever introduced. Note the test `application.conf` sets this to 3, so app
    * tests cap at 3 fibers — preserved by [[cappedFor]]'s small-pool guard.
    */
  private lazy val dbMaxPoolSize: Int = {
    val config = ConfigFactory.load()
    val path   = "database.pool.maximumPoolSize"
    if (config.hasPath(path)) config.getInt(path) else Int.MaxValue
  }

  /** Connections held back from API fan-out so a DB-phase burst can't check out the entire pool and starve
    * health/readiness probes or any concurrent ad-hoc query (which would otherwise queue on `connectionTimeout`).
    */
  private[utils] val PoolReserve: Int = 2

  /** Pure cap arithmetic, extracted from [[fiberCap]] so the rule is deterministically testable without
    * `ConfigFactory`. Returns the smaller of two ceilings:
    *
    *   1. '''API gate''' — `2 * maxPermits`. Over-provisions the gate so it stays saturated while fibers prep the next
    *      unit of work (DB writes, JSON decode) between requests, without queuing hundreds of fibers behind it. See
    *      commit 6706d5ef.
    *   2. '''DB pool''' — the pool size, minus a small [[PoolReserve]] margin so a fan-out burst can never check out
    *      every connection and leave none for health/readiness probes or ad-hoc queries.
    *
    * The one non-obvious part is the reserve's '''small-pool guard'''. Blindly subtracting the reserve would wreck a
    * small pool — the size-3 test pool would become `3 - 2 = 1`, silently serialising fan-out that used to run 3-wide.
    * So the reserve is taken '''only when the pool is more than twice the reserve''' (i.e. big enough that giving up
    * the margin still leaves the majority for fan-out); at or below that threshold the full pool is used. Net effect:
    * fan-out always keeps at least half the pool, so concurrency can never collapse, and only genuinely roomy pools pay
    * the margin. Worked examples (reserve = 2):
    *
    * {{{
    *   maxPermits  pool          result   why
    *   16          20            18       gate 32 vs 20-2=18  -> pool-reserve wins (the case #91 is about)
    *    8          20            16       gate 16 vs 18       -> gate wins; reserve doesn't bite
    *    4           3             3       3 <= 2*2, guard keeps full pool (test pool: behaviour unchanged)
    *   16           4             4       4 <= 2*2, still at the threshold -> full pool
    *   16           5             3       5 > 2*2 -> 5-2=3
    *   16     Int.MaxValue       32       no pool cap -> gate wins (no overflow: subtraction, not addition)
    * }}}
    *
    * The threshold makes the DB ceiling slightly non-monotonic right at the boundary — pool 4->4, 5->3, 6->4 — i.e. a
    * one-step dip in the 4-6 range. That's the deliberate cost of pinning the size-3 test pool to its full width; it
    * never bites real deployments (prod pool is 20).
    */
  private[utils] def cappedFor(maxPermits: Int, dbPool: Int): Int = {
    // Reserve the margin only when the pool can spare it (> 2*reserve); otherwise keep the whole pool so a tiny pool
    // isn't strangled. This keeps fan-out at >= half the pool in every case — see the worked examples above.
    val dbCeiling = if (dbPool <= 2 * PoolReserve) dbPool else dbPool - PoolReserve
    // Floor at 1: `withParallelism(0)` is invalid. Unreachable today (Hikari rejects pool 0), purely defensive.
    math.max(1, math.min(maxPermits * 2, dbCeiling))
  }

  /** Recommended cap on concurrent fibers for API-bound `foreachPar` against the given client. A fiber holds a pooled
    * connection only for the span of each `connectZIO` / `transactZIO`, so fibers may safely exceed connections, but
    * capping near the pool size keeps a single app from self-starving the pool during a DB-phase burst — see [[cappedFor]].
    *
    * With the prod config (`maxPermits` 16 — env `recovery-tiers` ending at 16; the checked-in default `[2,4,6,8]`
    * gives 8 — and pool 20) this is `min(32, 20 - 2) = 18`.
    */
  def fiberCap(client: ChessComClient): Int = cappedFor(client.maxPermits, dbMaxPoolSize)
}
