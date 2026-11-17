package ccas.utils

import java.io.EOFException
import java.util.concurrent.Semaphore

import zio.{durationInt, Fiber, Task, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

/** [[Abandonable.run]] over reads that stand in for stdin: `acquireUninterruptibly` ignores interruption, as a blocked
  * stdin read does.
  */
object TestAbandonable extends ZIOSpecDefault {

  /** A read that blocks until `input` is released, signalling `started` once it is under way. */
  private def stubbornRead(started: Semaphore, input: Semaphore): Task[String] =
    Abandonable.run {
      started.release()
      input.acquireUninterruptibly()
      "too late"
    }

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
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential
}
