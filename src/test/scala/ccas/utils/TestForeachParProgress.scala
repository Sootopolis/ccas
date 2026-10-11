package ccas.utils

import zio.{durationInt, Chunk, Ref, ZIO, ZLayer}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

object TestForeachParProgress extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("foreachParProgress")(
    testAllItemsProcessed,
    testResultsInItemOrder,
    testEmptyIterable,
    testActionFailurePropagated,
    testCounterReachesTotal
  ).provideShared(ZLayer.succeed(ProgressDisplay.make(enabled = false)))

  private def testAllItemsProcessed = test("processes all items") {
    ZIO.scoped {
      for {
        visited <- Ref.make(Set.empty[Int])
        items = (1 to 10).toList
        _ <- ProgressDisplay.foreachParProgress(items, 4)((n, total) => s"$n/$total") { i =>
          visited.update(_ + i)
        }
        result <- visited.get
      } yield assertTrue(result == (1 to 10).toSet)
    }
  }

  private def testResultsInItemOrder = test("returns the results in the items' order") {
    ZIO.scoped {
      for {
        // Later items finish first, so the order they complete in is not the order they were given in.
        results <- ProgressDisplay.foreachParProgress((1 to 8).toList, 4)((n, total) => s"$n/$total") { i =>
          ZIO.sleep((8 - i).millis).as(i * 10)
        }
      } yield assertTrue(results == Chunk.from((1 to 8).map(_ * 10)))
    }
  } @@ TestAspect.withLiveClock

  private def testEmptyIterable = test("handles empty iterable") {
    ZIO.scoped {
      for {
        _ <- ProgressDisplay.foreachParProgress(List.empty[Int], 4)((n, total) => s"$n/$total") { _ =>
          ZIO.fail(new RuntimeException("should not be called"))
        }
      } yield assertTrue(true)
    }
  }

  private def testActionFailurePropagated = test("propagates action failure") {
    val effect = ZIO.scoped {
      ProgressDisplay.foreachParProgress(List(1, 2, 3), 4)((n, total) => s"$n/$total") { i =>
        ZIO.when(i == 2)(ZIO.fail(new RuntimeException("boom")))
      }
    }
    effect.exit.map(exit => assertTrue(exit.isFailure))
  }

  private def testCounterReachesTotal = test("counter reaches total on success") {
    ZIO.scoped {
      for {
        counter <- Ref.make(0)
        items = (1 to 5).toList
        _ <- ProgressDisplay.foreachParProgress(items, 4)((n, _) => s"$n") { _ =>
          counter.update(_ + 1)
        }
        finalCount <- counter.get
      } yield assertTrue(finalCount == 5)
    }
  }
}
