package ccas.analysis.tables

import java.time.Instant

import com.augustnagro.magnum.*

import ccas.utils.sql.AdvisoryLock
import ccas.utils.sql.AdvisoryLock.Space
import ccas.utils.sql.DbCodecs.given

/** How writers moving the same names between holders queue, not race (ADR 0020). A writer holds its whole space in
  * shared mode and each of its names exclusively, or the whole space exclusively when it moves more names than one
  * transaction's share of the lock table should hold.
  */
private[tables] enum NameLock(nameSpace: Space, wholeSpace: Space) {
  case PlayerNames extends NameLock(nameSpace = Space.PlayerName, wholeSpace = Space.AllPlayerNames)
  case ClubNames   extends NameLock(nameSpace = Space.ClubName, wholeSpace = Space.AllClubNames)

  /** Locks `names` until the transaction ends and returns the database clock read once they are held, so a writer that
    * queued behind another stamps later. Call it once per transaction: each call takes its keys in order, but a second
    * call's can sort below the first's, and then two writers can wait on each other in a cycle.
    */
  def acquire(names: List[String])(using DbTx): Instant = {
    val keys = names.map(_.hashCode).distinct
    if (keys.size > NameLock.MaxNameKeys) { AdvisoryLock.acquire(wholeSpace, List(0)) }
    else {
      AdvisoryLock.acquireShared(wholeSpace, 0)
      AdvisoryLock.acquire(nameSpace, keys)
    }
    sql"SELECT clock_timestamp()".query[Instant].run().head
  }
}

private[tables] object NameLock {

  // Postgres sizes its shared lock table at `max_locks_per_transaction` per connection. A call that would take more
  // name locks than half of the default share locks the whole space instead.
  val MaxNameKeys = 32
}
