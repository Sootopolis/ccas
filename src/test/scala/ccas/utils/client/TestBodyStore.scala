package ccas.utils.client

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters.*

import com.typesafe.config.ConfigFactory
import zio.{Cause, Chunk, FiberId, FiberRefs, LogLevel, LogSpan, Ref, Scope, Trace, UIO, ZEnvironment, ZIO, ZLogger}
import zio.config.magnolia.DeriveConfig
import zio.config.typesafe.TypesafeConfigProvider
import zio.test.*

/** Unit coverage for [[FsBodyStore]] — the local-filesystem [[BodyStore]] that is the dev/test/CI double for the R2
  * backing store (#191) — plus the companion's fs-root resolution and error-degrading accessors (#200). Each test
  * gets its own temp root so runs don't interfere.
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

  /** Runs `body` with an extra logger attached for the duration, handing it an effect that reads back exactly what
    * was logged inside. Counting entries out of the shared `ZTestLogger` instead would tie the assertion to how the
    * spec happens to be wired (its buffer is per-test only while `TestEnvironment` is), and to sibling tests, which
    * emit the very same "BodyStore unavailable" text.
    */
  private def capturingLogs[R, E, A](body: UIO[Chunk[(LogLevel, String)]] => ZIO[R, E, A]): ZIO[R, E, A] = {
    val entries = new ConcurrentLinkedQueue[(LogLevel, String)]()
    val capture = new ZLogger[String, Unit] {
      def apply(
        trace: Trace,
        fiberId: FiberId,
        logLevel: LogLevel,
        message: () => String,
        cause: Cause[Any],
        context: FiberRefs,
        spans: List[LogSpan],
        annotations: Map[String, String]
      ): Unit = {
        entries.add(logLevel -> message())
        ()
      }
    }
    ZIO.scoped[R](ZIO.withLoggerScoped(capture) *> body(ZIO.succeed(Chunk.fromIterable(entries.asScala.toVector))))
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BodyStore")(
    suiteFs,
    suiteFsRootResolution,
    suiteDegradation
  )

  private def suiteFs = suite("FsBodyStore")(
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

  /** #200: the `fs` root must not be a CWD-relative literal (the old `cache/bodies` dropped runtime blobs into
    * whatever directory the process started in, usually the repo). Tests the pure resolution so no environment
    * rebinding is needed — the env read itself is a one-liner at the callsite.
    */
  private def suiteFsRootResolution = suite("fs-root resolution")(
    test("an explicit fs-root wins over the XDG default, relative or not") {
      // A relative override is honoured rather than rejected — it was deliberate. `buildFs` is what absolutises it
      // against the launch directory and warns, so this stays a pure, CWD-independent function.
      val absolute = BodyStore.resolveFsRoot(Some("/srv/ccas-bodies"), Some("/xdg-cache"), Some("/home/u"))
      val relative = BodyStore.resolveFsRoot(Some("cache/bodies"), Some("/xdg-cache"), Some("/home/u"))
      assertTrue(
        absolute == Right(Paths.get("/srv/ccas-bodies")),
        relative == Right(Paths.get("cache/bodies")),
        relative.exists(!_.isAbsolute)
      )
    },
    test("absent fs-root falls back to $XDG_CACHE_HOME/ccas/bodies") {
      val resolved = BodyStore.resolveFsRoot(None, Some("/xdg-cache"), Some("/home/u"))
      assertTrue(resolved == Right(Paths.get("/xdg-cache", "ccas", "bodies")))
    },
    test("absent fs-root and absent XDG_CACHE_HOME fall back to $HOME/.cache/ccas/bodies") {
      val resolved = BodyStore.resolveFsRoot(None, None, Some("/home/u"))
      assertTrue(resolved == Right(Paths.get("/home/u/.cache", "ccas", "bodies")))
    },
    test("no configured root and no resolvable home is a Left, never a relative guess") {
      // A null `user.home` would interpolate to a directory literally named "null", relative to the CWD — the very
      // CWD-dependence #200 removes. Fail the layer with an actionable message instead of guessing.
      val missing = BodyStore.resolveFsRoot(None, None, None)
      val blank   = BodyStore.resolveFsRoot(Some("  "), Some(""), Some(" "))
      assertTrue(
        missing.left.exists(_.contains("CCAS_BODY_STORE_FS_ROOT")),
        blank.isLeft
      )
    },
    test("an absent fs-root key decodes to None instead of failing the layer") {
      // `application.conf` now ships `fs-root = ${?CCAS_BODY_STORE_FS_ROOT}` with no literal, so with the env var
      // unset the key is absent from the resolved config entirely — i.e. every default install takes this path.
      val hocon    = ConfigFactory.parseString("""body-store { backend = "fs" }""")
      val provider = TypesafeConfigProvider.fromTypesafeConfig(hocon, enableCommaSeparatedValueAsList = true)
      for {
        config <- provider.load(summon[DeriveConfig[BodyStore.BodyStoreConfig]].desc.nested("body-store"))
      } yield assertTrue(config.backend == "fs", config.fsRoot.isEmpty)
    },
    test("blank values are treated as absent, never as the CWD") {
      // `Paths.get("")` resolves to the current directory — exactly the CWD-dependence #200 removes — so an
      // exported-but-empty CCAS_BODY_STORE_FS_ROOT must fall through to the default rather than be honoured.
      val blankRoot = BodyStore.resolveFsRoot(Some("   "), Some("/xdg-cache"), Some("/home/u"))
      val blankXdg  = BodyStore.resolveFsRoot(None, Some(""), Some("/home/u"))
      assertTrue(
        blankRoot == Right(Paths.get("/xdg-cache", "ccas", "bodies")),
        blankXdg == Right(Paths.get("/home/u/.cache", "ccas", "bodies")),
        blankRoot.exists(_.isAbsolute),
        blankXdg.exists(_.isAbsolute)
      )
    }
  )

  /** #200: store errors must degrade to a cache miss / skipped write, never propagate. The body cache is not source
    * of truth, so an outage has to take the cache offline rather than the app.
    */
  private def suiteDegradation = suite("error degradation")(
    test("getOrMiss reports a store error as a cache miss") {
      for {
        (store, _) <- freshStore
        faulty     <- FaultyBodyStore.make(store)
        _          <- store.put(hash, bytes)
        _          <- faulty.breakReads
        degraded   <- BodyStore.getOrMiss(hash).provideEnvironment(ZEnvironment[BodyStore](faulty))
        _          <- faulty.healReads
        healed     <- BodyStore.getOrMiss(hash).provideEnvironment(ZEnvironment[BodyStore](faulty))
      } yield assertTrue(degraded.isEmpty, healed.exists(_.sameElements(bytes)))
    },
    test("putOrSkip returns false on a store error and true once healed") {
      for {
        (store, _) <- freshStore
        faulty     <- FaultyBodyStore.make(store)
        _          <- faulty.breakWrites
        skipped    <- BodyStore.putOrSkip(hash, bytes).provideEnvironment(ZEnvironment[BodyStore](faulty))
        absent     <- store.get(hash)
        _          <- faulty.healWrites
        stored     <- BodyStore.putOrSkip(hash, bytes).provideEnvironment(ZEnvironment[BodyStore](faulty))
        present    <- store.get(hash)
      } yield assertTrue(!skipped, absent.isEmpty, stored, present.exists(_.sameElements(bytes)))
    },
    test("a broken store never fails the accessor's effect") {
      for {
        (store, _) <- freshStore
        faulty     <- FaultyBodyStore.make(store)
        _          <- faulty.breakReads
        _          <- faulty.breakWrites
        env        = ZEnvironment[BodyStore](faulty)
        readExit   <- BodyStore.getOrMiss(hash).provideEnvironment(env).exit
        writeExit  <- BodyStore.putOrSkip(hash, bytes).provideEnvironment(env).exit
      } yield assertTrue(readExit.isSuccess, writeExit.isSuccess)
    },
    test("an outage logs one warning however long it lasts, and one info on recovery") {
      // The anti-spam guarantee: without transition-tracking, an R2 outage on a fetch-heavy run emits a line per
      // failed read AND per failed write — thousands of them — burying the signal. Five failing operations here
      // must produce exactly one WARN, and the return to health exactly one INFO.
      capturingLogs { sink =>
        for {
          (store, _) <- freshStore
          faulty     <- FaultyBodyStore.make(store)
          degraded   <- Ref.make(false)
          monitored   = new BodyStore.HealthLogging(faulty, degraded)
          _          <- faulty.breakReads
          _          <- faulty.breakWrites
          _          <- ZIO.foreachDiscard(1 to 3)(_ => monitored.get(hash).ignore)
          _          <- ZIO.foreachDiscard(1 to 2)(_ => monitored.put(hash, bytes).ignore)
          _          <- faulty.healReads
          _          <- faulty.healWrites
          _          <- ZIO.foreachDiscard(1 to 3)(_ => monitored.get(hash).ignore)
          entries    <- sink
          warnings    = entries.count((level, msg) => level == LogLevel.Warning && msg.contains("BodyStore unavailable"))
          recoveries  = entries.count((level, msg) => level == LogLevel.Info && msg.contains("BodyStore recovered"))
        } yield assertTrue(warnings == 1, recoveries == 1)
      }
    },
    test("the health decorator re-raises the underlying error rather than absorbing it") {
      // Observability only: turning failures into cache misses stays the accessors' job, so the two concerns can
      // move independently (#199 adds a budget-driven skip at the accessor layer).
      for {
        (store, _) <- freshStore
        faulty     <- FaultyBodyStore.make(store)
        degraded   <- Ref.make(false)
        monitored   = new BodyStore.HealthLogging(faulty, degraded)
        _          <- faulty.breakReads
        exit       <- monitored.get(hash).exit
      } yield assertTrue(exit.isFailure)
    }
  )
}
