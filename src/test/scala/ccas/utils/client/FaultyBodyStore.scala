package ccas.utils.client

import java.io.IOException

import zio.{Ref, Task, UIO, ZIO}

/** A [[BodyStore]] decorator whose reads and writes can be failed independently at any point in a test, simulating an
  * object-store outage (R2 unreachable, credentials rotated, disk unreadable) rather than a merely-absent object.
  *
  * The distinction matters: a missing object was always handled (`None` → refetch), but a store *error* used to
  * propagate and fail the request. #200 made both degrade to a cache miss, and these are the tests for it. Reads and
  * writes break separately because the two degradations are different — a read outage must refetch, a write outage
  * must return the in-memory value uncached.
  */
final class FaultyBodyStore private (delegate: BodyStore, readsDown: Ref[Boolean], writesDown: Ref[Boolean])
    extends BodyStore {

  def get(hash: String): Task[Option[Array[Byte]]] = gate(readsDown, "get")(delegate.get(hash))

  def put(hash: String, bytes: Array[Byte]): Task[Unit] = gate(writesDown, "put")(delegate.put(hash, bytes))

  // Deletes ride on the write flag: an outage takes the whole store down, not one verb.
  def delete(hash: String): Task[Unit] = gate(writesDown, "delete")(delegate.delete(hash))

  val breakReads: UIO[Unit]  = readsDown.set(true)
  val healReads: UIO[Unit]   = readsDown.set(false)
  val breakWrites: UIO[Unit] = writesDown.set(true)
  val healWrites: UIO[Unit]  = writesDown.set(false)

  private def gate[A](flag: Ref[Boolean], op: String)(effect: Task[A]): Task[A] =
    flag.get.flatMap { down =>
      if (down) { ZIO.fail(new IOException(s"simulated body-store outage on $op")) }
      else { effect }
    }
}

object FaultyBodyStore {

  /** Wrap a working store (typically the suite's [[FsBodyStore]]); starts healthy so a test can populate real data
    * before breaking it.
    */
  def make(delegate: BodyStore): UIO[FaultyBodyStore] =
    for {
      readsDown  <- Ref.make(false)
      writesDown <- Ref.make(false)
    } yield new FaultyBodyStore(delegate, readsDown, writesDown)
}
