package ccas.server.jobs

import zio.*
import zio.stream.ZStream
import zio.test.{assertTrue, Spec, TestClock, ZIOSpecDefault}

/** Verifies the `/logs` keepalive heartbeat (#150): real lines pass through untouched, the stream still halts the
  * moment its source completes ("stream close = job done"), and a silent gap longer than the interval yields keepalive
  * frames so the follower's connection never idles shut. Driven entirely by `TestClock` — no wall-clock dependency.
  */
object TestJobLogStream extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobLogStream")(
    test("withKeepAlive passes real lines through and halts when the source completes") {
      for {
        // A finite source completes at virtual t=0, before the first keepalive tick fires — mergeHaltLeft halts with it.
        out <- JobLogStream.withKeepAlive(ZStream("a", "b")).runCollect
      } yield assertTrue(out.filterNot(_ == JobLogStream.KeepAliveLine) == Chunk("a", "b"))
    },
    test("withKeepAlive injects a keepalive during a silent gap and halts once the source resumes and ends") {
      for {
        gate <- Promise.make[Nothing, Unit]
        seen <- Ref.make(Chunk.empty[String])
        // "a", then silence until the gate opens, then "final", then complete.
        source = ZStream("a") ++ ZStream.fromZIO(gate.await).drain ++ ZStream("final")
        fiber <- JobLogStream.withKeepAlive(source).runForeach(s => seen.update(_ :+ s)).fork
        // Wait until the merge is live (first real line observed) so the keepalive tick's sleep is registered, then
        // advance past the interval — the silent gap must produce at least one keepalive.
        _   <- seen.get.repeatUntil(_.contains("a"))
        _   <- TestClock.adjust(JobLogStream.KeepAliveInterval * 3)
        _   <- seen.get.repeatUntil(_.exists(_ == JobLogStream.KeepAliveLine))
        _   <- gate.succeed(())
        _   <- fiber.join
        out <- seen.get
      } yield assertTrue(
        out.count(_ == JobLogStream.KeepAliveLine) >= 1,
        out.filterNot(_ == JobLogStream.KeepAliveLine) == Chunk("a", "final")
      )
    }
  )
}
