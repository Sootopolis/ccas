package ccas.cli

import java.io.{ByteArrayOutputStream, PrintStream}

import zio.{Ref, ZIO}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.utils.{BarSnapshot, ProgressDisplay, ProgressSnapshot}

/** Unit tests for [[ClientProgressRenderer]] — the reconcile-and-draw of `/progress` snapshot frames onto a local
  * display. Uses `ProgressDisplay.makeWith` with a capture buffer (no `System.out` swap), so bar output is observable
  * without touching process-global streams.
  */
object TestClientProgressRenderer extends ZIOSpecDefault {

  private def withRenderer[A](
    use: (ClientProgressRenderer, Ref[Map[Int, ClientProgressRenderer.Tracked]]) => ZIO[Any, Throwable, A]
  ): ZIO[Any, Throwable, (A, String)] =
    ZIO.suspendSucceed {
      val baos    = new ByteArrayOutputStream
      val ps      = new PrintStream(baos, true, "UTF-8")
      val display = ProgressDisplay.makeWith(enabled = true, ps, ps, None)
      Ref.make(Map.empty[Int, ClientProgressRenderer.Tracked]).flatMap { ref =>
        use(new ClientProgressRenderer(display, ref), ref).map { a =>
          ps.flush()
          (a, baos.toString("UTF-8"))
        }
      }
    }

  private def frame(bars: BarSnapshot*): ProgressSnapshot = ProgressSnapshot(bars.toList)

  override def spec: Spec[Any, Throwable] = suite("TestClientProgressRenderer")(
    test("renders a new bar, re-renders on update, and re-derives the block bar at the client from raw fields") {
      withRenderer { (renderer, ref) =>
        for {
          _        <- renderer.render(frame(BarSnapshot(0, 5, 10, "Working")))
          afterAdd <- ref.get
          _        <- renderer.render(frame(BarSnapshot(0, 8, 10, "Working")))
        } yield afterAdd
      }.map { case (afterAdd, out) =>
        assertTrue(
          afterAdd.keySet == Set(0),        // tracked under the server-assigned bar id
          out.contains("Working"),
          out.contains("50.0%"),            // 5/10 rendered client-side
          out.contains("80.0%"),            // updated to 8/10
          out.contains("█"),                // block bar drawn locally from raw current/total
          out.contains("░")
        )
      }
    },
    test("a bar absent from the next frame is finished and forgotten (implicit removal)") {
      withRenderer { (renderer, ref) =>
        for {
          _           <- renderer.render(frame(BarSnapshot(0, 1, 10, "A"), BarSnapshot(1, 2, 10, "B")))
          afterTwo    <- ref.get
          _           <- renderer.render(frame(BarSnapshot(1, 3, 10, "B"))) // bar 0 dropped
          afterRemove <- ref.get
        } yield (afterTwo, afterRemove)
      }.map { case ((afterTwo, afterRemove), _) =>
        assertTrue(
          afterTwo.keySet == Set(0, 1),
          afterRemove.keySet == Set(1) // only the surviving bar remains tracked
        )
      }
    },
    test("clear finishes every remaining bar and forgets them") {
      withRenderer { (renderer, ref) =>
        for {
          _         <- renderer.render(frame(BarSnapshot(0, 1, 10, "A"), BarSnapshot(1, 2, 10, "B")))
          _         <- renderer.clear
          afterClear <- ref.get
        } yield afterClear
      }.map { case (afterClear, _) =>
        assertTrue(afterClear.isEmpty)
      }
    },
    test("logLine is interleaved on the display's stream") {
      withRenderer { (renderer, _) =>
        renderer.render(frame(BarSnapshot(0, 1, 10, "Bar"))) *> renderer.logLine("a log line")
      }.map { case (_, out) =>
        assertTrue(out.contains("a log line"), out.contains("Bar"))
      }
    },
    test("re-rendering an identical frame writes nothing further (unchanged bars are not redrawn)") {
      ZIO.suspendSucceed {
        val baos    = new ByteArrayOutputStream
        val ps      = new PrintStream(baos, true, "UTF-8")
        val display = ProgressDisplay.makeWith(enabled = true, ps, ps, None)
        for {
          ref <- Ref.make(Map.empty[Int, ClientProgressRenderer.Tracked])
          renderer = new ClientProgressRenderer(display, ref)
          _              <- renderer.render(frame(BarSnapshot(0, 5, 10, "X")))
          _              <- ZIO.succeed(ps.flush())
          lenAfterFirst  <- ZIO.succeed(baos.size())
          _              <- renderer.render(frame(BarSnapshot(0, 5, 10, "X"))) // byte-identical frame
          _              <- ZIO.succeed(ps.flush())
          lenAfterSecond <- ZIO.succeed(baos.size())
        } yield assertTrue(lenAfterSecond == lenAfterFirst)
      }
    }
  )
}
