package ccas.utils.sql

import com.augustnagro.magnum.*

/** The only place this codebase takes a Postgres advisory lock, and the registry of the numbers that keep one kind of
  * lock apart from another. Postgres gives a lock's numbers no meaning, so two features that picked the same [[Space]]
  * would wait on, deadlock with, or misread each other's locks, and nothing else would notice. `TestAdvisoryLock` fails
  * on a number used twice and on an advisory-lock call anywhere else (ADR 0020). Each method takes a `DbTx` because
  * these locks last until the transaction ends, and outside one they would end with their own statement.
  */
object AdvisoryLock {

  /** A number names a kind of lock to every build running against a database, so add entries but never change or reuse
    * a number. The `0x0CCA` prefix keeps clear of the small numbers a writer unaware of this file would pick.
    */
  enum Space(val classId: Int) {
    case PlayerName     extends Space(0x0CCA0001)
    case AllPlayerNames extends Space(0x0CCA0002)
    case ClubName       extends Space(0x0CCA0003)
    case AllClubNames   extends Space(0x0CCA0004)
  }

  /** Locks each of `keys` in `space` exclusively, in key order, so two calls taking overlapping keys never wait on each
    * other in a cycle.
    */
  def acquire(space: Space, keys: List[Int])(using DbTx): Unit =
    keys.distinct.sorted.foreach { key =>
      sql"SELECT 1 FROM pg_advisory_xact_lock(${space.classId}, $key) AS held".query[Int].run()
    }

  /** Locks `key` in `space` in shared mode: it waits only for, and holds off only, an exclusive holder. */
  def acquireShared(space: Space, key: Int)(using DbTx): Unit = {
    sql"SELECT 1 FROM pg_advisory_xact_lock_shared(${space.classId}, $key) AS held".query[Int].run()
    ()
  }
}
