package ccas.utils

import java.nio.file.Paths

import org.postgresql.ds.PGSimpleDataSource
import zio.{Task, ZEnvironment, ZIO}
import zio.http.Routes
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.utils.client.{BodyStore, ChessComClient, FsBodyStore, TestChessComClientSupport}
import ccas.utils.sql.{PostgresClient, TestDatabases}

/** Checks of [[ApiConcurrency.cappedFor]] — the `min(gate, pool − reserve)` rule with its small-pool guard, every case a
  * hand-computed expectation — and of where [[ApiConcurrency.fiberCap]] takes the pool's size from. No database is
  * reached: the pool below is lazy and holds no idle connection.
  */
object TestApiConcurrency extends ZIOSpecDefault {

  import ApiConcurrency.{cappedFor, PoolReserve}

  override def spec: Spec[Any, Throwable] = suite("TestApiConcurrency")(
    test("prod config: pool is the binding ceiling, reserve is held back (20 → 18)") {
      // maxPermits 16 → gate 32; pool 20 > 2*reserve → 20 − 2 = 18; min(32, 18) = 18.
      assertTrue(cappedFor(maxPermits = 16, dbPool = 20) == 18)
    },
    test("gate-bound: small maxPermits wins, reserve never bites (pool 20 → 16)") {
      // maxPermits 8 → gate 16; pool ceiling 18; min(16, 18) = 16, so the reserve makes no difference here.
      assertTrue(cappedFor(maxPermits = 8, dbPool = 20) == 16)
    },
    test("tiny test pool keeps its full size — small-pool guard (3 → 3)") {
      // The size-3 test pool: 3 <= 2*reserve, so NO reserve is taken; min(8, 3) = 3, unchanged from pre-#91.
      assertTrue(cappedFor(maxPermits = 4, dbPool = 3) == 3)
    },
    test("boundary: pool == 2*reserve keeps the full pool") {
      // At the threshold the guard still declines to reserve, so fan-out keeps all of a just-big-enough pool.
      assertTrue(cappedFor(maxPermits = 16, dbPool = 2 * PoolReserve) == 2 * PoolReserve)
    },
    test("just above boundary: reserve begins to apply") {
      val pool = 2 * PoolReserve + 1
      assertTrue(cappedFor(maxPermits = 16, dbPool = pool) == pool - PoolReserve)
    },
    test("large pool stays gate-bound (100 → 32)") {
      assertTrue(cappedFor(maxPermits = 16, dbPool = 100) == 32)
    },
    test("no DB cap known: falls back to 2*maxPermits, no overflow (Int.MaxValue → 32)") {
      // fiberCap passes Int.MaxValue for a pool of unknown size; Int.MaxValue − reserve never wins the min.
      assertTrue(cappedFor(maxPermits = 16, dbPool = Int.MaxValue) == 32)
    },
    test("fiberCap takes the database ceiling from the pool the client writes through") {
      // A pool of 10 caps at 8 against a gate of 32; the test config's pool of 3 would cap at 3.
      ZIO.scoped {
        for {
          config <- TestDatabases.configured
          pool <- PostgresClient.make(
            config.copy(pool = config.pool.copy(maximumPoolSize = 10, minimumIdle = 0)),
            schema = None,
            onInit = ZIO.unit
          )
          client <- clientOver(pool)
        } yield assertTrue(ApiConcurrency.fiberCap(client) == 8)
      }
    },
    test("a pool of unknown size leaves only the gate's ceiling") {
      for {
        client <- clientOver(TestDatabases.over(new PGSimpleDataSource()))
      } yield assertTrue(ApiConcurrency.fiberCap(client) == 32)
    },
    test("defensive floor: never returns 0 even for a degenerate pool (0 → 1)") {
      // Unreachable in practice (Hikari rejects pool 0), but withParallelism(0) is invalid, so floor at 1.
      assertTrue(cappedFor(maxPermits = 16, dbPool = 0) == 1)
    },
    test("invariant: fan-out always gets at least half the pool, whatever the reserve") {
      // With an effectively-infinite gate the result is the DB ceiling alone; the small-pool guard guarantees it
      // never drops below half the pool, so concurrency can never collapse to a near-serial trickle.
      val pools = List(3, 4, 5, 8, 20, 100)
      assertTrue(pools.forall(pool => cappedFor(maxPermits = 1_000_000, dbPool = pool) * 2 >= pool))
    }
  )

  // A client whose gate tops out at 16 permits, writing through `pool`; it never makes a request.
  private def clientOver(pool: PostgresClient): Task[ChessComClient] =
    TestChessComClientSupport
      .fakeClient(Routes.empty, permits = 16)
      .provideEnvironment(
        ZEnvironment[PostgresClient, BodyStore](pool, new FsBodyStore(Paths.get("target/test-bodies")))
      )
}
