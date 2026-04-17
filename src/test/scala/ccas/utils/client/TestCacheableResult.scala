package ccas.utils.client

import zio.*
import zio.test.*

import ccas.analysis.tables.subtypes.ApiResponseBodyId

/** Pure unit tests for the `foldZIO` and `unlessUnchangedDiscard` helpers on `CacheableResult`. No HTTP client, no DB
  * — these just construct each variant and assert the branch dispatch and laziness contracts.
  */
object TestCacheableResult extends ZIOSpecDefault {

  private val bodyId1 = ApiResponseBodyId.wrap(1L)
  private val bodyId2 = ApiResponseBodyId.wrap(2L)
  private val bodyId3 = ApiResponseBodyId.wrap(3L)

  private val explodingValue: Task[Int] =
    ZIO.fail(new RuntimeException("getValue must not be forced"))

  override def spec: Spec[TestEnvironment, Any] = suite("CacheableResult helpers")(
    suite("foldZIO")(
      test("Fresh dispatches to ifUnchanged and exposes the variant's bodyId") {
        val fresh: CacheableResult[Int] = CacheableResult.Fresh(bodyId1, explodingValue)
        for {
          r <- fresh.foldZIO(u => ZIO.succeed(s"unchanged-${u.bodyId.value}"))(_ => ZIO.succeed("changed"))
        } yield assertTrue(r == "unchanged-1")
      },
      test("Revalidated dispatches to ifUnchanged") {
        val reval: CacheableResult[Int] = CacheableResult.Revalidated(bodyId2, explodingValue)
        for {
          r <- reval.foldZIO(u => ZIO.succeed(u.bodyId.value))(_ => ZIO.succeed(-1L))
        } yield assertTrue(r == 2L)
      },
      test("IdenticalBody dispatches to ifUnchanged") {
        val ident: CacheableResult[Int] = CacheableResult.IdenticalBody(bodyId3, explodingValue)
        for {
          r <- ident.foldZIO(u => ZIO.succeed(u.bodyId.value))(_ => ZIO.succeed(-1L))
        } yield assertTrue(r == 3L)
      },
      test("Changed dispatches to ifChanged and passes the decoded value") {
        val changed: CacheableResult[Int] = CacheableResult.Changed(42)
        for {
          r <- changed.foldZIO(_ => ZIO.succeed(-1))(v => ZIO.succeed(v * 2))
        } yield assertTrue(r == 84)
      },
      test("getValue is never forced on the unchanged branch") {
        // If foldZIO accidentally forced getValue, `explodingValue` would fail the effect.
        val fresh: CacheableResult[Int] = CacheableResult.Fresh(bodyId1, explodingValue)
        for {
          r <- fresh.foldZIO(_ => ZIO.succeed("skipped"))(_ => ZIO.succeed("ran"))
        } yield assertTrue(r == "skipped")
      }
    ),
    suite("unlessUnchangedDiscard")(
      test("skips the effect for every Unchanged variant") {
        for {
          counter <- Ref.make(0)
          bump     = counter.update(_ + 1)
          _ <- (CacheableResult.Fresh(bodyId1, explodingValue): CacheableResult[Int]).unlessUnchangedDiscard(bump)
          _ <- (CacheableResult.Revalidated(bodyId2, explodingValue): CacheableResult[Int]).unlessUnchangedDiscard(bump)
          _ <- (CacheableResult.IdenticalBody(bodyId3, explodingValue): CacheableResult[Int]).unlessUnchangedDiscard(bump)
          v <- counter.get
        } yield assertTrue(v == 0)
      },
      test("runs the effect on Changed and discards its result") {
        val changed: CacheableResult[Int] = CacheableResult.Changed(42)
        for {
          counter <- Ref.make(0)
          unit    <- changed.unlessUnchangedDiscard(counter.update(_ + 7).as("non-unit-result-discarded"))
          v       <- counter.get
        } yield assertTrue(v == 7, unit == ())
      }
    )
  )
}
