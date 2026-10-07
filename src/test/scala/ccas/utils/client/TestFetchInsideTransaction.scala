package ccas.utils.client

import zio.*
import zio.http.*
import zio.test.*

import ccas.analysis.tables.Tables
import ccas.utils.client.TestChessComClientSupport.*
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.withTransaction

/** A Chess.com fetch holds a transaction's connection and locks open for its round trip, so the client refuses one on a
  * fiber inside `withTransaction` (#300).
  */
object TestFetchInsideTransaction extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] = suite("TestFetchInsideTransaction")(
    testFetchInsideDies,
    testFetchAfterTransactionSucceeds,
    testFetchOnForkedFiberDies,
    testCachedBodyLoadInsideDies
  ).provideShared(
    FreshSchemaLayer("test_fetch_inside_transaction", Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def url(name: String): URL = testUrl.addPath(name)

  /** Serves [[jsonBody]] with a `max-age`, so a second fetch of the same URL is a `Fresh` cache hit. */
  private def countingRoutes(calls: Ref[Int]): Routes[Any, Response] =
    Routes(
      Method.GET / "api" / string("name") -> handler { (_: String, _: Request) =>
        calls.update(_ + 1).as(Response.json(jsonBody).addHeader(Header.CacheControl.MaxAge(300)))
      }
    )

  private def diedNaming(exit: Exit[Throwable, Any], fetched: URL): Boolean =
    exit match {
      case Exit.Failure(cause) => cause.defects.exists(_.getMessage.contains(fetched.encode))
      case _                   => false
    }

  private def testFetchInsideDies =
    test("a fetch inside withTransaction dies naming the URL, before any request is sent") {
      for {
        calls  <- Ref.make(0)
        client <- fakeClient(countingRoutes(calls))
        exit   <- withTransaction(client.get[Payload](url("inside"))).exit
        sent   <- calls.get
      } yield assertTrue(diedNaming(exit, url("inside")), sent == 0)
    }

  private def testFetchAfterTransactionSucceeds =
    test("once a transaction has ended, even by dying, the same fiber can fetch") {
      for {
        calls   <- Ref.make(0)
        client  <- fakeClient(countingRoutes(calls))
        _       <- withTransaction(client.get[Payload](url("refused"))).exit
        payload <- client.get[Payload](url("after"))
        sent    <- calls.get
      } yield assertTrue(payload == Payload("ok"), sent == 1)
    }

  private def testFetchOnForkedFiberDies =
    test("a fetch on a fiber forked inside withTransaction dies too") {
      for {
        calls  <- Ref.make(0)
        client <- fakeClient(countingRoutes(calls))
        exit   <- withTransaction(client.get[Payload](url("forked")).fork.flatMap(_.join)).exit
        sent   <- calls.get
      } yield assertTrue(diedNaming(exit, url("forked")), sent == 0)
    }

  private def testCachedBodyLoadInsideDies =
    test("loading a cached body inside withTransaction dies, though the cache hit was taken outside") {
      for {
        calls  <- Ref.make(0)
        client <- fakeClient(countingRoutes(calls))
        _      <- client.get[Payload](url("cached"))
        result <- client.getResult[Payload](url("cached"))
        exit   <- withTransaction(result.getValue).exit
        sent   <- calls.get
      } yield assertTrue(result.isInstanceOf[FetchResult.Fresh[?]], sent == 1, diedNaming(exit, url("cached")))
    }
}
