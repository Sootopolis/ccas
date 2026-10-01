package ccas.utils.sql

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.sql
import zio.{durationInt, Promise, ZIO}
import zio.test.Live

import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient.{connectZIO, withTransaction}

/** Makes two writers overlap every time, rather than only when the scheduler happens to interleave them. */
object ForcedOverlap {

  /** What `second` waited on, as `pg_stat_activity.wait_event` names it (`advisory`, `transactionid`, …), and the
    * database clock once `first` was let go: anything `second` stamps after its wait is later than that.
    */
  final case class Overlap(waitedOn: String, releasedAt: Instant)

  /** Runs `first` in a transaction held open until `second` is seen waiting on a lock, then lets both finish. Dies if
    * `second` finishes without waiting, since then the two did not contend and the test would prove nothing. Holds
    * three pool connections at once.
    */
  def run(
    first: ZIO[PostgresClient, SQLException, Any],
    second: ZIO[PostgresClient, SQLException, Any]
  ): ZIO[PostgresClient, SQLException, Overlap] =
    for {
      holding     <- Promise.make[Nothing, Unit]
      release     <- Promise.make[Nothing, Unit]
      firstFiber  <- withTransaction(first *> holding.succeed(()) *> release.await *> clock).fork
      _           <- holding.await.raceFirst(firstFiber.join.unit)
      secondFiber <- second.fork
      waitedOn <- awaitLockWaiter.raceFirst(
        secondFiber.await.flatMap(exit => ZIO.dieMessage(s"the second writer never waited on the first: $exit"))
      )
      _          <- release.succeed(())
      releasedAt <- firstFiber.join
      _          <- secondFiber.join
    } yield Overlap(waitedOn, releasedAt)

  private val clock: ZIO[PostgresClient, SQLException, Instant] =
    connectZIO(sql"SELECT clock_timestamp()".query[Instant].run().head)

  // Any waiter is `second`: suites run one at a time, and nothing else in the suite runs while this does.
  private val awaitLockWaiter: ZIO[PostgresClient, SQLException, String] = {
    val waiting = connectZIO {
      sql"""SELECT wait_event FROM pg_stat_activity
            WHERE datname = current_database() AND wait_event_type = 'Lock'""".query[String].run().headOption
    }
    def poll: ZIO[PostgresClient, SQLException, String] = waiting.someOrElseZIO(ZIO.sleep(10.millis) *> poll)
    Live.live(poll.timeout(30.seconds)).someOrElseZIO(ZIO.dieMessage("the second writer neither waited nor finished"))
  }
}
