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
    * assumes the default `"database"` prefix, so a `PostgresClient.live(prefix = …)` built under another prefix is not
    * seen here and the DB cap falls away; (2) when the path is absent it yields `Int.MaxValue` (no DB cap, so `fiberCap`
    * reverts to `2 * maxPermits`). Note the test `application.conf` sets this to 3, so app tests cap at 3 fibers.
    */
  private lazy val dbMaxPoolSize: Int = {
    val config = ConfigFactory.load()
    val path   = "database.pool.maximumPoolSize"
    if (config.hasPath(path)) config.getInt(path) else Int.MaxValue
  }

  /** Recommended cap on concurrent fibers for API-bound `foreachPar` against the given client. The lower of two
    * ceilings:
    *   - '''API gate''' — `2 * maxPermits`. Over-provisions the gate so it stays saturated while fibers prep the next
    *     unit of work (DB writes, JSON decode) between requests, without queuing hundreds of fibers behind it. See
    *     commit 6706d5ef.
    *   - '''DB pool''' — `database.pool.maximumPoolSize`. A fiber holds a pooled connection only for the span of each
    *     `connectZIO` / `transactZIO`, so fibers may safely exceed connections, but capping at the pool size keeps a
    *     single app from self-starving the pool during a DB-phase burst.
    *
    * With the default config (`maxPermits` 16, pool 20) this is `min(32, 20) = 20`.
    */
  def fiberCap(client: ChessComClient): Int = math.min(client.maxPermits * 2, dbMaxPoolSize)
}
