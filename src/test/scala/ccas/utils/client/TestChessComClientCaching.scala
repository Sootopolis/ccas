package ccas.utils.client

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.*
import zio.http.*
import zio.test.*

import ccas.analysis.tables.{ApiResponseBody, ApiResponseCache, Tables}
import ccas.analysis.tables.subtypes.ApiResponseBodyId
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.client.TestChessComClientSupport.*
import ccas.utils.sql.PostgresClient.connectZIO

object TestChessComClientCaching extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] = suite("TestChessComClientCaching")(
    suiteCacheable
  ).provideShared(
    FreshSchemaLayer("test_client_caching", Tables.ensureTables)
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.seconds)

  /** Build a `Response` with typed Cache-Control / ETag / Content-Type headers so the cache-parsing path is
    * exercised end-to-end. `maxAge = None` + `noStore = false` + `noCache = false` produces no Cache-Control header
    * at all. `noStore` or `noCache` combine with `maxAge` via `Header.CacheControl.Multiple` so the parser's
    * directive-walking path is also covered. `lastModified` is attached as a raw custom header so tests can inject
    * Chess.com's non-RFC date format verbatim and verify `HttpDate.parse` handles it.
    */
  private def cacheable200(
    body: String,
    maxAge: Option[Int] = Some(300),
    etag: Option[String] = Some("v1"),
    lastModified: Option[String] = None,
    noStore: Boolean = false,
    noCache: Boolean = false
  ): Response = {
    val directives: Vector[Header.CacheControl] =
      maxAge.map(n => Header.CacheControl.MaxAge(n)).toVector ++
        (if (noStore) Vector(Header.CacheControl.NoStore) else Vector.empty) ++
        (if (noCache) Vector(Header.CacheControl.NoCache) else Vector.empty)
    val ccHeader: Option[Header.CacheControl] = directives match {
      case Vector()  => None
      case Vector(d) => Some(d)
      case ds        => Some(Header.CacheControl.Multiple(NonEmptyChunk.fromIterable(ds.head, ds.tail)))
    }
    val etagHeader: Option[Header.ETag] = etag.map(Header.ETag.Strong(_))
    val allHeaders = Headers(Header.ContentType(MediaType.application.json)) ++
      ccHeader.fold(Headers.empty)(h => Headers(h)) ++
      etagHeader.fold(Headers.empty)(h => Headers(h)) ++
      lastModified.fold(Headers.empty)(lm => Headers(Header.Custom("Last-Modified", lm)))
    Response(status = Status.Ok, headers = allHeaders, body = Body.fromString(body))
  }

  /** Build a 304 Not Modified response with optional refreshed headers. Used by the metadata-refresh tests to
    * verify `handleNotModified` merges fresh `Cache-Control` / ETag / Last-Modified values from the 304 into the
    * stored cache entry.
    */
  private def notModified304(
    maxAge: Option[Int],
    etag: Option[String],
    lastModified: Option[String] = None,
    noCache: Boolean = false
  ): Response = {
    val directives: Vector[Header.CacheControl] =
      maxAge.map(n => Header.CacheControl.MaxAge(n)).toVector ++
        (if (noCache) Vector(Header.CacheControl.NoCache) else Vector.empty)
    val ccHeader: Option[Header.CacheControl] = directives match {
      case Vector()  => None
      case Vector(d) => Some(d)
      case ds        => Some(Header.CacheControl.Multiple(NonEmptyChunk.fromIterable(ds.head, ds.tail)))
    }
    val etagHeader: Option[Header.ETag] = etag.map(Header.ETag.Strong(_))
    val allHeaders = Headers.empty ++
      ccHeader.fold(Headers.empty)(h => Headers(h)) ++
      etagHeader.fold(Headers.empty)(h => Headers(h)) ++
      lastModified.fold(Headers.empty)(lm => Headers(Header.Custom("Last-Modified", lm)))
    Response(status = Status.NotModified, headers = allHeaders)
  }

  private def suiteCacheable = suite("cacheable dispatch")(
    test("first call returns Changed and populates cache") {
      val url = URL.decode("http://test.example.com/api/cacheable/changed-first").toOption.get
      ZIO.scoped {
        for {
          (client, _, stats) <- makeClient(_ => ZIO.succeed(cacheable200(jsonBody)))
          result <- client.getCacheable[Payload](url)
          value  <- result.getValue
          meta   <- ApiResponseCache.lookupMeta(url.encode)
          s      <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Changed[?]],
          !result.isUnchanged,
          value.value == "ok",
          meta.exists(_.etag.contains("\"v1\"")),
          meta.exists(_.maxAgeSeconds.contains(300L)),
          s.requests == 1L,
          s.cacheMisses == 1L,
          s.cacheHits == 0L,
          s.cacheRevalidations == 0L
        )
      }
    },
    test("second call within max-age returns Fresh without a network call") {
      val url = URL.decode("http://test.example.com/api/cacheable/fresh-hit").toOption.get
      ZIO.scoped {
        for {
          netCalls <- Ref.make(0)
          (client, _, stats) <- makeClient { _ =>
            netCalls.update(_ + 1).as(cacheable200(jsonBody, maxAge = Some(3600)))
          }
          _      <- client.getCacheable[Payload](url) // populate cache
          result <- client.getCacheable[Payload](url) // should Fresh
          value  <- result.getValue
          calls  <- netCalls.get
          s      <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Fresh[?]],
          result.isUnchanged,
          value.value == "ok",
          calls == 1, // only the first populate call hit the network
          s.requests == 1L,
          s.cacheHits == 1L
        )
      }
    },
    test("stale entry with 304 response returns Revalidated and touches fetched_at") {
      val url = URL.decode("http://test.example.com/api/cacheable/revalidated").toOption.get
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, stats) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(jsonBody, maxAge = Some(0)) // immediately stale on read
              else Response(status = Status.NotModified)
            }
          }
          _         <- client.getCacheable[Payload](url)
          before    <- ApiResponseCache.lookupMeta(url.encode)
          _         <- ZIO.sleep(20.millis) // ensure touched fetched_at differs
          result    <- client.getCacheable[Payload](url)
          value     <- result.getValue
          after     <- ApiResponseCache.lookupMeta(url.encode)
          s         <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Revalidated[?]],
          result.isUnchanged,
          value.value == "ok",
          before.exists(b => after.exists(_.fetchedAt.isAfter(b.fetchedAt))),
          s.cacheRevalidations == 1L,
          s.cacheHits == 0L,
          // 304 responses are attributed to the current permit tier in attemptsByTier, same as any other request —
          // the tier counter increments inside rawGet before the network call, so the 304 branch inherits it.
          s.attemptsByTier.values.sum == 2L // one populate attempt + one revalidation attempt
        )
      }
    },
    test("stale entry with 200 identical body returns IdenticalBody") {
      val url = URL.decode("http://test.example.com/api/cacheable/identical").toOption.get
      ZIO.scoped {
        for {
          // Always return the same body with a zero max-age so the cache row is always stale
          (client, _, stats) <- makeClient(_ => ZIO.succeed(cacheable200(jsonBody, maxAge = Some(0))))
          _      <- client.getCacheable[Payload](url) // populate cache
          result <- client.getCacheable[Payload](url) // server returns identical body
          value  <- result.getValue
          s      <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.IdenticalBody[?]],
          result.isUnchanged,
          value.value == "ok",
          s.requests == 2L,      // both calls hit the network
          s.cacheMisses == 1L,   // first call
          s.cacheHits == 1L      // second call — IdenticalBody increments cacheHits
        )
      }
    },
    test("stale entry with 200 different body returns Changed and replaces the cache row") {
      val url = URL.decode("http://test.example.com/api/cacheable/changed-on-revalidate").toOption.get
      val body1 = """{"value":"first"}"""
      val body2 = """{"value":"second"}"""
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, stats) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(body1, maxAge = Some(0), etag = Some("v1"))
              else cacheable200(body2, maxAge = Some(0), etag = Some("v2"))
            }
          }
          _            <- client.getCacheable[Payload](url)
          firstBodyId  <- ApiResponseCache.lookupMeta(url.encode).map(_.get.bodyId)
          result       <- client.getCacheable[Payload](url)
          value        <- result.getValue
          secondBodyId <- ApiResponseCache.lookupMeta(url.encode).map(_.get.bodyId)
          s            <- stats.get
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Changed[?]],
          !result.isUnchanged,
          value.value == "second",
          firstBodyId != secondBodyId,
          s.cacheMisses == 2L
        )
      }
    },
    test("Cache-Control: no-store response is not cached") {
      val url = URL.decode("http://test.example.com/api/cacheable/no-store").toOption.get
      ZIO.scoped {
        for {
          (client, _, _) <- makeClient(_ => ZIO.succeed(cacheable200(jsonBody, noStore = true)))
          result <- client.getCacheable[Payload](url)
          _      <- result.getValue
          meta   <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(
          result.isInstanceOf[CacheableResult.Changed[?]],
          meta.isEmpty
        )
      }
    },
    test("response without Cache-Control is never fresh (always revalidates or misses)") {
      val url = URL.decode("http://test.example.com/api/cacheable/no-cache-control").toOption.get
      ZIO.scoped {
        for {
          netCalls <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            netCalls.update(_ + 1).as(cacheable200(jsonBody, maxAge = None, etag = Some("only-etag")))
          }
          _      <- client.getCacheable[Payload](url) // populates cache without max-age
          result <- client.getCacheable[Payload](url) // should NOT be Fresh — server returned 200 with same body
          _      <- result.getValue
          calls  <- netCalls.get
        } yield assertTrue(
          // Without max-age the entry is never fresh; second call hits the network. With the same body returned,
          // ApiResponseBody dedupes by SHA-256 and we get IdenticalBody.
          result.isInstanceOf[CacheableResult.IdenticalBody[?]],
          calls == 2
        )
      }
    },
    test("Cache-Control: no-cache strips max-age so entries are always revalidated") {
      val url = URL.decode("http://test.example.com/api/cacheable/no-cache").toOption.get
      ZIO.scoped {
        for {
          netCalls <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            // Server sends no-cache alongside an hour-long max-age. Per RFC 7234 §5.2.2.2, the no-cache directive
            // wins: we must revalidate before reuse regardless of the max-age value.
            netCalls.update(_ + 1).as(cacheable200(jsonBody, maxAge = Some(3600), noCache = true))
          }
          _      <- client.getCacheable[Payload](url) // populates cache
          meta   <- ApiResponseCache.lookupMeta(url.encode)
          result <- client.getCacheable[Payload](url) // must hit the network despite max-age=3600
          _      <- result.getValue
          calls  <- netCalls.get
        } yield assertTrue(
          meta.exists(_.maxAgeSeconds.isEmpty), // no-cache stripped the max-age at persist time
          !result.isInstanceOf[CacheableResult.Fresh[?]],
          calls == 2
        )
      }
    },
    test("conditional request attaches If-None-Match in wire format (quotes preserved)") {
      val url = URL.decode("http://test.example.com/api/cacheable/if-none-match").toOption.get
      ZIO.scoped {
        for {
          lastHeaders <- Ref.make(Headers.empty)
          (client, _, _) <- makeClient { req =>
            lastHeaders.set(req.headers).as(cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc")))
          }
          _   <- client.getCacheable[Payload](url) // populate cache with etag
          _   <- client.getCacheable[Payload](url) // second call sends If-None-Match
          hs  <- lastHeaders.get
          inm = hs.rawHeader("If-None-Match")
        } yield assertTrue(
          // Chess.com expects quoted etag on the wire (RFC 7232). Bare "abc" would be rejected.
          inm.contains("\"abc\"")
        )
      }
    },
    test("Fresh whose body was pruned mid-flight falls through to a network refetch") {
      // Simulates a retention race: a caller receives CacheableResult.Fresh, holds the lazy load, and between
      // `lookupMeta` and `getValue` the body row gets deleted (e.g. by ApiResponseCache.deleteBefore on another
      // app's startup). loadAndDecode should treat the missing body as a cache miss and fetch fresh data.
      // Use a body that's unique to this test so api_response_body.deleteOrphans can actually remove it —
      // the shared jsonBody is referenced by cache rows from sibling tests and would be preserved.
      val url        = URL.decode("http://test.example.com/api/cacheable/race-delete").toOption.get
      val uniqueBody = """{"value":"race-delete-unique"}"""
      ZIO.scoped {
        for {
          netCalls <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            netCalls.update(_ + 1).as(cacheable200(uniqueBody, maxAge = Some(3600)))
          }
          _     <- client.getCacheable[Payload](url)               // populates cache (network call #1)
          fresh <- client.getCacheable[Payload](url)               // Fresh hit; body not loaded yet
          // Simulate the race: cascade-delete the cache row and its body row.
          _     <- ApiResponseCache.invalidate(url.encode)
          _     <- ApiResponseBody.deleteOrphans
          value <- fresh.getValue                                  // must recover via recursive get[T] (network #2)
          calls <- netCalls.get
        } yield assertTrue(
          fresh.isInstanceOf[CacheableResult.Fresh[?]],
          value.value == "race-delete-unique",
          calls == 2
        )
      }
    },
    test("Chess.com-format Last-Modified is parsed to an Instant and stored") {
      val url             = URL.decode("http://test.example.com/api/cacheable/lm-chess-com").toOption.get
      val chessComFormat  = "Thursday, 16-Apr-2026 23:13:22 GMT+0000"
      val expectedInstant = Instant.parse("2026-04-16T23:13:22Z")
      ZIO.scoped {
        for {
          (client, _, _) <- makeClient(_ => ZIO.succeed(
            cacheable200(jsonBody, lastModified = Some(chessComFormat))
          ))
          _    <- client.getCacheable[Payload](url)
          meta <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(meta.exists(_.lastModified.contains(expectedInstant)))
      }
    },
    test("conditional request sends If-Modified-Since in IMF-fixdate form regardless of received format") {
      val url            = URL.decode("http://test.example.com/api/cacheable/if-modified-since").toOption.get
      val chessComFormat = "Thursday, 16-Apr-2026 23:13:22 GMT+0000"
      ZIO.scoped {
        for {
          lastHeaders <- Ref.make(Headers.empty)
          (client, _, _) <- makeClient { req =>
            lastHeaders.set(req.headers).as(
              cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc"), lastModified = Some(chessComFormat))
            )
          }
          _   <- client.getCacheable[Payload](url)
          _   <- client.getCacheable[Payload](url)
          hs  <- lastHeaders.get
          ims = hs.rawHeader("If-Modified-Since")
        } yield assertTrue(
          ims.contains("Thu, 16 Apr 2026 23:13:22 GMT")
        )
      }
    },
    test("response without Last-Modified leaves column NULL and omits If-Modified-Since on revalidation") {
      val url = URL.decode("http://test.example.com/api/cacheable/lm-absent").toOption.get
      ZIO.scoped {
        for {
          lastHeaders <- Ref.make(Headers.empty)
          (client, _, _) <- makeClient { req =>
            lastHeaders.set(req.headers).as(cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc")))
          }
          _    <- client.getCacheable[Payload](url)
          _    <- client.getCacheable[Payload](url)
          meta <- ApiResponseCache.lookupMeta(url.encode)
          hs   <- lastHeaders.get
          ims   = hs.rawHeader("If-Modified-Since")
        } yield assertTrue(
          meta.exists(_.lastModified.isEmpty),
          ims.isEmpty
        )
      }
    },
    test("304 with Cache-Control: max-age=3600 overwrites the stored max_age_seconds") {
      val url = URL.decode("http://test.example.com/api/cacheable/304-max-age-bump").toOption.get
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc"))
              else notModified304(maxAge = Some(3600), etag = Some("abc"))
            }
          }
          _    <- client.getCacheable[Payload](url)
          _    <- client.getCacheable[Payload](url)
          meta <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(meta.exists(_.maxAgeSeconds.contains(3600L)))
      }
    },
    test("304 with Cache-Control: no-cache clears the stored max_age_seconds") {
      val url = URL.decode("http://test.example.com/api/cacheable/304-no-cache").toOption.get
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc"))
              else notModified304(maxAge = Some(3600), etag = Some("abc"), noCache = true)
            }
          }
          _    <- client.getCacheable[Payload](url)
          _    <- client.getCacheable[Payload](url)
          meta <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(meta.exists(_.maxAgeSeconds.isEmpty))
      }
    },
    test("304 with no refresh headers preserves stored metadata, only touches fetched_at") {
      val url             = URL.decode("http://test.example.com/api/cacheable/304-no-refresh").toOption.get
      val chessComFormat  = "Thursday, 16-Apr-2026 23:13:22 GMT+0000"
      val expectedInstant = Instant.parse("2026-04-16T23:13:22Z")
      ZIO.scoped {
        for {
          callCount <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            callCount.getAndUpdate(_ + 1).map { n =>
              if (n == 0) cacheable200(jsonBody, maxAge = Some(0), etag = Some("abc"), lastModified = Some(chessComFormat))
              else Response(status = Status.NotModified)
            }
          }
          _      <- client.getCacheable[Payload](url)
          before <- ApiResponseCache.lookupMeta(url.encode)
          _      <- ZIO.sleep(20.millis) // ensure touched fetched_at differs measurably
          _      <- client.getCacheable[Payload](url)
          after  <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(
          before.isDefined,
          after.isDefined,
          after.get.fetchedAt.isAfter(before.get.fetchedAt),
          after.get.etag == before.get.etag,
          after.get.lastModified.contains(expectedInstant),
          after.get.maxAgeSeconds == before.get.maxAgeSeconds,
          after.get.contentType == before.get.contentType
        )
      }
    },
    test("cached body that no longer parses (schema drift) triggers invalidate + refetch") {
      val url         = URL.decode("http://test.example.com/api/cacheable/schema-drift").toOption.get
      val beforeBody  = """{"value":"schema-drift-before"}"""
      val refetchBody = """{"value":"schema-drift-after"}"""
      ZIO.scoped {
        for {
          netCalls  <- Ref.make(0)
          stage     <- Ref.make(0)
          (client, _, _) <- makeClient { _ =>
            stage.getAndUpdate(_ + 1).flatMap { s =>
              netCalls.update(_ + 1).as(
                // First call populates the cache with the original body; after the UPDATE-in-place below corrupts
                // that row, the recovery refetch should see a new body and replace the cache row.
                if (s == 0) cacheable200(beforeBody, maxAge = Some(3600))
                else cacheable200(refetchBody, maxAge = Some(3600))
              )
            }
          }
          _     <- client.getCacheable[Payload](url)
          fresh <- client.getCacheable[Payload](url)             // Fresh hit; body not loaded yet
          bodyId = fresh.asInstanceOf[CacheableResult.Fresh[Payload]].bodyId
          // Corrupt the cached body so the next decode throws JsonDecodingException. Unique body content means
          // this UPDATE only affects the row we created for this URL — no other test shares it.
          _     <- connectZIO(
            sql"UPDATE api_response_body SET body = '{\"oops\":true}' WHERE body_id = ${ApiResponseBodyId.unwrap(bodyId)}".update.run()
          )
          value <- fresh.getValue                                // decode fails → invalidate + refetch from network
          calls <- netCalls.get
          meta  <- ApiResponseCache.lookupMeta(url.encode)
        } yield assertTrue(
          value.value == "schema-drift-after",
          calls == 2,                                            // first populate + recovery refetch
          meta.isDefined                                         // refetch re-populated the cache
        )
      }
    }
  )
}
