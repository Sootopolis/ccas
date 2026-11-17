package ccas.utils

import java.io.EOFException
import java.util.concurrent.{LinkedBlockingQueue, Semaphore}
import java.util.concurrent.atomic.AtomicInteger

import zio.{durationInt, Fiber, Task, UIO, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

/** [[Abandonable.run]] and [[Abandonable.keeping]] over reads that stand in for stdin: `acquireUninterruptibly` ignores
  * interruption, as a blocked stdin read does, and a [[Typist]] read blocks until a line is typed.
  */
object TestAbandonable extends ZIOSpecDefault {

  /** A read that blocks until `input` is released, signalling `started` once it is under way. */
  private def stubbornRead(started: Semaphore, input: Semaphore): Task[String] =
    Abandonable.run {
      started.release()
      input.acquireUninterruptibly()
      "too late"
    }

  /** Stands in for an operator at stdin: each read blocks until a line is typed, and is counted as it starts. */
  private final class Typist {
    private val typed   = new LinkedBlockingQueue[String]()
    private val started = new Semaphore(0)
    val reads           = new AtomicInteger(0)

    def read(): String = {
      reads.incrementAndGet()
      started.release()
      typed.take()
    }

    def readStarted: Task[Unit]        = ZIO.attemptBlocking(started.acquire())
    def types(line: String): UIO[Unit] = ZIO.succeed(typed.put(line))
  }

  /** Reads that `typist` answers, kept, and an asker already waiting on one. */
  private final case class Waiting(reads: Task[String], asker: Fiber.Runtime[Throwable, String])

  /** Returns once the asker is parked on its read, so an interrupt lands in the wait and not on the way to it. */
  private def waitingAsker(typist: Typist): Task[Waiting] =
    for {
      reads <- Abandonable.keeping(typist.read())
      asker <- reads.fork
      _     <- typist.readStarted
      _ <- (ZIO.sleep(1.millis) *> asker.status).repeatUntil {
        case _: Fiber.Status.Suspended => true
        case _                         => false
      }
    } yield Waiting(reads, asker)

  override def spec: Spec[Any, Throwable] = suite("TestAbandonable")(
    test("interrupting a read that ignores interruption ends the fiber at once, rather than waiting for input") {
      val started = new Semaphore(0)
      val input   = new Semaphore(0)
      for {
        fiber <- stubbornRead(started, input).fork
        _     <- ZIO.attemptBlocking(started.acquire())
        exit  <- fiber.interrupt.timeout(5.seconds)
        _     <- ZIO.succeed(input.release()) // frees the abandoned thread
      } yield assertTrue(exit.exists(_.isInterruptedOnly))
    },
    // At exit ZIOApp interrupts every live root fiber and waits for each, as this does. `.disconnect` passes the test
    // above and fails this one: its read goes on in a root fiber of its own, which held Ctrl-C for 15 s.
    test("an interrupted read leaves no fiber behind for the app's shutdown to wait on") {
      val started = new Semaphore(0)
      val input   = new Semaphore(0)
      for {
        before <- Fiber.roots
        fiber  <- stubbornRead(started, input).fork
        _      <- ZIO.attemptBlocking(started.acquire())
        _      <- fiber.interrupt
        after  <- Fiber.roots
        leftOver = after.filterNot(before.contains)
        shutdown <- Fiber.interruptAll(leftOver).timeout(5.seconds)
        _        <- ZIO.succeed(input.release()) // frees the abandoned thread
      } yield assertTrue(shutdown.isDefined)
    },
    test("a read that answers gives its line") {
      for {
        line <- Abandonable.run("y")
      } yield assertTrue(line == "y")
    },
    test("a read that throws fails with what it threw") {
      val ended = new EOFException("no more input")
      for {
        failure <- Abandonable.run(throw ended).flip
      } yield assertTrue(failure == ended)
    },
    test("a kept read interrupted while it waits gives its line to the next read, not to nobody") {
      val typist = new Typist
      for {
        waiting    <- waitingAsker(typist)
        _          <- waiting.asker.interrupt
        _          <- typist.types("y")
        lineOption <- waiting.reads.timeout(5.seconds)
      } yield assertTrue(lineOption.contains("y"))
    },
    test("a read asked for while an abandoned one is still under way waits for that one, starting no second") {
      val typist = new Typist
      for {
        waiting    <- waitingAsker(typist)
        _          <- waiting.asker.interrupt
        next       <- waiting.reads.fork
        _          <- typist.types("y")
        lineOption <- next.join.timeout(5.seconds)
      } yield assertTrue(lineOption.contains("y"), typist.reads.get == 1)
    },
    test("a kept read's line, once taken, is not given again") {
      val typist = new Typist
      for {
        reads  <- Abandonable.keeping(typist.read())
        _      <- typist.types("a") *> typist.types("b")
        first  <- reads
        second <- reads
      } yield assertTrue(first == "a", second == "b", typist.reads.get == 2)
    },
    test("two reads asked for at once get a line each") {
      val typist = new Typist
      for {
        reads       <- Abandonable.keeping(typist.read())
        one         <- reads.fork
        other       <- reads.fork
        _           <- typist.types("a") *> typist.types("b")
        linesOption <- one.join.zip(other.join).timeout(5.seconds)
      } yield assertTrue(linesOption.exists { case (a, b) => Set(a, b) == Set("a", "b") })
    },
    test("an interrupted kept read leaves no fiber behind for the app's shutdown to wait on") {
      val typist = new Typist
      for {
        before  <- Fiber.roots
        waiting <- waitingAsker(typist)
        _       <- waiting.asker.interrupt
        after   <- Fiber.roots
        leftOver = after.filterNot(before.contains)
        shutdown <- Fiber.interruptAll(leftOver).timeout(5.seconds)
        _        <- typist.types("") // frees the abandoned thread
      } yield assertTrue(shutdown.isDefined)
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential
}
