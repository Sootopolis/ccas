package ccas.utils.client

import java.io.IOException

import zio.{Duration, Ref, Task, UIO, ZIO}

/** A [[BodyStore]] decorator whose reads and writes can be failed '''or stalled''' independently at any point in a
  * test, simulating an object-store outage (R2 unreachable, credentials rotated, disk unreadable) or a brownout
  * (#211) rather than a merely-absent object.
  *
  * The distinction matters: a missing object was always handled (`None` → refetch), but a store *error* used to
  * propagate and fail the request. #200 made both degrade to a cache miss, and these are the tests for it. Reads and
  * writes break separately because the two degradations are different — a read outage must refetch, a write outage
  * must return the in-memory value uncached.
  *
  * Stalls are deliberately `attemptBlocking` + `Thread.sleep` rather than `ZIO.sleep`: the failure being tested is a
  * store whose blocking call does not honour `Thread.interrupt` (a `UrlConnectionHttpClient` socket read, a hung
  * network mount). An interruptible `ZIO.sleep` would be cut short by a plain `.timeout` too, so it could not tell a
  * correct `.disconnect`-based deadline from a broken one.
  */
final class FaultyBodyStore private (
  delegate: BodyStore,
  readsDown: Ref[Boolean],
  writesDown: Ref[Boolean],
  readStall: Ref[Duration],
  writeStall: Ref[Duration]
) extends BodyStore {

  def get(hash: String): Task[Option[Array[Byte]]] = stall(readStall) *> gate(readsDown, "get")(delegate.get(hash))

  def put(hash: String, bytes: Array[Byte]): Task[Unit] =
    stall(writeStall) *> gate(writesDown, "put")(delegate.put(hash, bytes))

  // Deletes ride on the write flag: an outage takes the whole store down, not one verb.
  def delete(hash: String): Task[Unit] = stall(writeStall) *> gate(writesDown, "delete")(delegate.delete(hash))

  val breakReads: UIO[Unit]  = readsDown.set(true)
  val healReads: UIO[Unit]   = readsDown.set(false)
  val breakWrites: UIO[Unit] = writesDown.set(true)
  val healWrites: UIO[Unit]  = writesDown.set(false)

  def stallReads(duration: Duration): UIO[Unit]  = readStall.set(duration)
  def stallWrites(duration: Duration): UIO[Unit] = writeStall.set(duration)

  private def stall(duration: Ref[Duration]): Task[Unit] =
    duration.get.flatMap { delay =>
      ZIO.whenDiscard(delay.toMillis > 0)(ZIO.attemptBlocking(Thread.sleep(delay.toMillis)))
    }

  private def gate[A](flag: Ref[Boolean], op: String)(effect: Task[A]): Task[A] =
    flag.get.flatMap { down =>
      if (down) { ZIO.fail(new IOException(s"simulated body-store outage on $op")) }
      else { effect }
    }
}

object FaultyBodyStore {

  /** Wrap a working store (typically the suite's [[FsBodyStore]]); starts healthy and responsive so a test can
    * populate real data before breaking or slowing it.
    */
  def make(delegate: BodyStore): UIO[FaultyBodyStore] =
    for {
      readsDown  <- Ref.make(false)
      writesDown <- Ref.make(false)
      readStall  <- Ref.make(Duration.Zero)
      writeStall <- Ref.make(Duration.Zero)
    } yield new FaultyBodyStore(delegate, readsDown, writesDown, readStall, writeStall)
}
