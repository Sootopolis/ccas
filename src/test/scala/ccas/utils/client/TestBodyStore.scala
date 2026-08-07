package ccas.utils.client

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import zio.{Scope, ZIO}
import zio.test.*

/** Unit coverage for [[FsBodyStore]] — the local-filesystem [[BodyStore]] that is the dev/test/CI double for the R2
  * backing store (#191). Each test gets its own temp root so runs don't interfere.
  */
object TestBodyStore extends ZIOSpecDefault {

  private def freshStore: ZIO[Scope, Throwable, (FsBodyStore, Path)] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("ccas-bodystore-test"))
    )(dir => ZIO.attemptBlocking(deleteRecursively(dir)).ignore)
      .map(dir => (new FsBodyStore(dir), dir))

  private def deleteRecursively(dir: Path): Unit =
    if (Files.exists(dir)) {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => { Files.deleteIfExists(p); () })
    }

  private val hash  = "a" * 64
  private val bytes = """{"value":"ok"}""".getBytes(StandardCharsets.UTF_8)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("FsBodyStore")(
    test("get on a missing key returns None") {
      for {
        (store, _) <- freshStore
        result     <- store.get(hash)
      } yield assertTrue(result.isEmpty)
    },
    test("put then get round-trips the exact bytes") {
      for {
        (store, _) <- freshStore
        _          <- store.put(hash, bytes)
        result     <- store.get(hash)
      } yield assertTrue(result.exists(_.sameElements(bytes)))
    },
    test("put shards the object under a two-char prefix directory") {
      for {
        (store, root) <- freshStore
        _             <- store.put(hash, bytes)
        exists        <- ZIO.attemptBlocking(Files.exists(root.resolve(hash.take(2)).resolve(hash)))
      } yield assertTrue(exists)
    },
    test("put is idempotent and last-write-wins for the same key") {
      val updated = """{"value":"updated"}""".getBytes(StandardCharsets.UTF_8)
      for {
        (store, _) <- freshStore
        _          <- store.put(hash, bytes)
        _          <- store.put(hash, updated)
        result     <- store.get(hash)
      } yield assertTrue(result.exists(_.sameElements(updated)))
    },
    test("delete removes the object; get then returns None") {
      for {
        (store, _) <- freshStore
        _          <- store.put(hash, bytes)
        _          <- store.delete(hash)
        result     <- store.get(hash)
      } yield assertTrue(result.isEmpty)
    },
    test("delete on a missing key is a no-op (idempotent)") {
      for {
        (store, _) <- freshStore
        _          <- store.delete(hash)
        again      <- store.delete(hash).exit
      } yield assertTrue(again.isSuccess)
    },
    test("distinct hashes do not collide") {
      val hashB  = "b" * 64
      val bytesB = "different".getBytes(StandardCharsets.UTF_8)
      for {
        (store, _) <- freshStore
        _          <- store.put(hash, bytes)
        _          <- store.put(hashB, bytesB)
        a          <- store.get(hash)
        b          <- store.get(hashB)
      } yield assertTrue(a.exists(_.sameElements(bytes)), b.exists(_.sameElements(bytesB)))
    }
  )
}
