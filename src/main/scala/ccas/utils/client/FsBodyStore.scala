package ccas.utils.client

import java.nio.file.{Files, NoSuchFileException, Path, StandardCopyOption}

import zio.{Task, ZIO}

/** Local-filesystem [[BodyStore]] laid out as `root/<hash[:2]>/<hash>` (the two-char shard keeps directory fan-out
  * bounded). Zero infrastructure — the local-dev default and the test double, so no R2 credentials are needed in CI.
  * All operations run on `attemptBlockingInterrupt` so a fiber blocked on disk I/O is interruptible on shutdown,
  * matching the JDBC idiom used elsewhere.
  */
final class FsBodyStore(root: Path) extends BodyStore {

  private def pathFor(hash: String): Path = root.resolve(hash.take(2)).resolve(hash)

  // Read directly and map an absent file to None, rather than exists-then-read: a concurrent delete between an
  // `exists` check and `readAllBytes` would otherwise surface as a `NoSuchFileException` failure instead of the
  // documented None -> cache-miss -> refetch fallthrough (matching S3BodyStore's NoSuchKeyException handling).
  def get(hash: String): Task[Option[Array[Byte]]] =
    ZIO
      .attemptBlockingInterrupt[Option[Array[Byte]]](Some(Files.readAllBytes(pathFor(hash))))
      .catchSome { case _: NoSuchFileException => ZIO.none }

  // Write to a sibling temp file then rename into place. A POSIX same-directory rename is atomic, so a concurrent
  // reader never observes a half-written body; concurrent puts of the same hash write byte-identical content, so
  // last-writer-wins is safe. Idempotent by construction (key = content hash).
  def put(hash: String, bytes: Array[Byte]): Task[Unit] =
    ZIO.attemptBlockingInterrupt {
      val path = pathFor(hash)
      Files.createDirectories(path.getParent)
      val tmp = Files.createTempFile(path.getParent, s".$hash", ".tmp")
      try {
        Files.write(tmp, bytes)
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING)
      } finally { Files.deleteIfExists(tmp) }
    }

  def delete(hash: String): Task[Unit] =
    ZIO.attemptBlockingInterrupt(Files.deleteIfExists(pathFor(hash))).unit
}
