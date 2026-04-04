package ccas.utils

import zio.{Ref, ZIO, ZLayer}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

object TestForeachParProgress extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("foreachParProgress")(
    testAllItemsProcessed,
    testEmptyIterable,
    testActionFailurePropagated,
    testCounterReachesTotal
  ).provideShared(ZLayer.succeed(TestCcasLogger.noop))

  private def testAllItemsProcessed = test("processes all items") {
    ZIO.scoped {
      for {
        visited <- Ref.make(Set.empty[Int])
        items = (1 to 10).toList
        _ <- CcasLogger.foreachParProgress(items)((n, total) => s"$n/$total") { i =>
          visited.update(_ + i)
        }
        result <- visited.get
      } yield assertTrue(result == (1 to 10).toSet)
    }
  }

  private def testEmptyIterable = test("handles empty iterable") {
    ZIO.scoped {
      for {
        _ <- CcasLogger.foreachParProgress(List.empty[Int])((n, total) => s"$n/$total") { _ =>
          ZIO.fail(new RuntimeException("should not be called"))
        }
      } yield assertTrue(true)
    }
  }

  private def testActionFailurePropagated = test("propagates action failure") {
    val effect = ZIO.scoped {
      CcasLogger.foreachParProgress(List(1, 2, 3))((n, total) => s"$n/$total") { i =>
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
        _ <- CcasLogger.foreachParProgress(items)((n, _) => s"$n") { _ =>
          counter.update(_ + 1)
        }
        finalCount <- counter.get
      } yield assertTrue(finalCount == 5)
    }
  }
}
