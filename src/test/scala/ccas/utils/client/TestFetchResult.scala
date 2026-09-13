package ccas.utils.client

import zio.*
import zio.test.*

import zio.http.URL

import ccas.analysis.tables.subtypes.ApiResponseBodyId
import ccas.utils.client.TestChessComClientSupport.reportedNotFoundBody

/** Pure unit tests for `FetchResult`'s folds and `getValue`. No HTTP client, no DB — these just construct each
  * variant and assert the branch dispatch, the laziness contract, and that absence reaches a caller as the
  * reported 404 rather than as a value.
  */
object TestFetchResult extends ZIOSpecDefault {

  private val bodyId1 = ApiResponseBodyId.wrap(1L)
  private val bodyId2 = ApiResponseBodyId.wrap(2L)
  private val bodyId3 = ApiResponseBodyId.wrap(3L)

  private val explodingValue: Task[Int] =
    ZIO.fail(new RuntimeException("getValue must not be forced"))

  private val missingUrl = URL.decode("http://test.example.com/api/gone").toOption.get
  private val notFound   = ReportedNotFound(missingUrl, reportedNotFoundBody)

  override def spec: Spec[TestEnvironment, Any] = suite("FetchResult helpers")(
    suite("foldZIO")(
      test("Fresh dispatches to ifUnchanged and exposes the variant's bodyId") {
        val fresh: FetchResult[Int] = FetchResult.Fresh(bodyId1, explodingValue)
        for {
          r <- fresh.foldZIO(
            ifMissing = _ => ZIO.succeed("missing"),
            ifUnchanged = u => ZIO.succeed(s"unchanged-${u.bodyId.value}"),
            ifChanged = _ => ZIO.succeed("changed")
          )
        } yield assertTrue(r == "unchanged-1")
      },
      test("Revalidated dispatches to ifUnchanged") {
        val reval: FetchResult[Int] = FetchResult.Revalidated(bodyId2, explodingValue)
        for {
          r <- reval.foldZIO(
            ifMissing = _ => ZIO.succeed(-2L),
            ifUnchanged = u => ZIO.succeed(u.bodyId.value),
            ifChanged = _ => ZIO.succeed(-1L)
          )
        } yield assertTrue(r == 2L)
      },
      test("IdenticalBody dispatches to ifUnchanged") {
        val ident: FetchResult[Int] = FetchResult.IdenticalBody(bodyId3, explodingValue)
        for {
          r <- ident.foldZIO(
            ifMissing = _ => ZIO.succeed(-2L),
            ifUnchanged = u => ZIO.succeed(u.bodyId.value),
            ifChanged = _ => ZIO.succeed(-1L)
          )
        } yield assertTrue(r == 3L)
      },
      test("Changed dispatches to ifChanged and passes the decoded value") {
        val changed: FetchResult[Int] = FetchResult.Changed(42, None)
        for {
          r <- changed.foldZIO(
            ifMissing = _ => ZIO.succeed(-2),
            ifUnchanged = _ => ZIO.succeed(-1),
            ifChanged = c => ZIO.succeed(c.value * 2)
          )
        } yield assertTrue(r == 84)
      },
      test("Missing dispatches to ifMissing and carries the reported 404") {
        val missing: FetchResult[Int] = FetchResult.Missing(notFound)
        for {
          r <- missing.foldZIO(
            ifMissing = m => ZIO.succeed(m.cause.statusCode),
            ifUnchanged = _ => ZIO.succeed(-1),
            ifChanged = _ => ZIO.succeed(-1)
          )
        } yield assertTrue(r == 404)
      },
      test("foldPresentZIO raises the reported 404 for Missing, so rename recovery still sees it") {
        val missing: FetchResult[Int] = FetchResult.Missing(notFound)
        for {
          e <- missing.foldPresentZIO(_ => ZIO.succeed(-1), _ => ZIO.succeed(-1)).either
        } yield assertTrue(e == Left(notFound))
      },
      test("getValue fails with the reported 404 for Missing") {
        val missing: FetchResult[Int] = FetchResult.Missing(notFound)
        for {
          e <- missing.getValue.either
        } yield assertTrue(e == Left(notFound))
      },
      test("getValue is never forced on the unchanged branch") {
        // If foldZIO accidentally forced getValue, `explodingValue` would fail the effect.
        val fresh: FetchResult[Int] = FetchResult.Fresh(bodyId1, explodingValue)
        for {
          r <- fresh.foldZIO(
            ifMissing = _ => ZIO.succeed("missing"),
            ifUnchanged = _ => ZIO.succeed("skipped"),
            ifChanged = _ => ZIO.succeed("ran")
          )
        } yield assertTrue(r == "skipped")
      }
    ),
    suite("map")(
      test("Fresh keeps its bodyId and the mapped getValue stays lazy") {
        val fresh: FetchResult[Int] = FetchResult.Fresh(bodyId1, explodingValue)
        val mapped                  = fresh.map(_.toString)
        for {
          r <- mapped.foldZIO(
            ifMissing = _ => ZIO.succeed("missing"),
            ifUnchanged = u => ZIO.succeed(s"unchanged-${u.bodyId.value}"),
            ifChanged = _ => ZIO.succeed("changed")
          )
        } yield assertTrue(r == "unchanged-1")
      },
      test("Changed applies f immediately, visible without going through getValue") {
        val changed: FetchResult[Int] = FetchResult.Changed(21, None)
        val mapped                    = changed.map(_ * 2)
        val value = mapped match {
          case FetchResult.Changed(v, _) => v
          case _                         => -1
        }
        assertTrue(value == 42)
      },
      test("Changed's getValue reflects the mapped value") {
        val changed: FetchResult[Int] = FetchResult.Changed(21, None)
        for {
          v <- changed.map(_ * 2).getValue
        } yield assertTrue(v == 42)
      },
      test("Missing passes through unchanged, still failing getValue with the reported 404") {
        val missing: FetchResult[Int] = FetchResult.Missing(notFound)
        for {
          e <- missing.map(_.toString).getValue.either
        } yield assertTrue(e == Left(notFound))
      }
    )
  )
}
