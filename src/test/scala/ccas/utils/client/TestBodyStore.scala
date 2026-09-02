package ccas.utils.client

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters.*

import com.typesafe.config.ConfigFactory
import zio.{Cause, Chunk, Exit, FiberId, FiberRefs, LogLevel, LogSpan, Ref, Scope, Trace, UIO, ZEnvironment, ZIO, ZLogger, durationInt}
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

  private val hash   = "a" * 64
  private val bytes  = """{"value":"ok"}""".getBytes(StandardCharsets.UTF_8)
  private val source = "https://api.chess.com/pub/player/example"

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
    suiteDegradation,
    suiteDeadlines,
    suiteTimeoutConfig
  )

  private def storeConfig(
    readTimeoutMs: Option[Int],
    writeTimeoutMs: Option[Int],
    s3ConnectTimeoutMs: Option[Int],
    s3SocketTimeoutMs: Option[Int]
  ): BodyStore.BodyStoreConfig =
    BodyStore.BodyStoreConfig(
      backend = "fs",
      fsRoot = None,
      readTimeoutMs = readTimeoutMs,
      writeTimeoutMs = writeTimeoutMs,
      s3Endpoint = None,
      s3Bucket = None,
      s3Region = None,
      s3AccessKey = None,
      s3SecretKey = None,
      s3ConnectTimeoutMs = s3ConnectTimeoutMs,
      s3SocketTimeoutMs = s3SocketTimeoutMs
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
    test("read separates an absent object (Missing) from a store that could not answer (Unavailable)") {
      // #215: both are cache misses the caller heals by refetching, but only `Missing` means the cache row pointing
      // here is a lie. Collapsing them is what made an outage delete the validators it would need on recovery.
      for {
        (store, _) <- freshStore
        faulty     <- FaultyBodyStore.make(store)
        env         = ZEnvironment[BodyStore](faulty)
        absent     <- BodyStore.read(hash).provideEnvironment(env)
        _          <- store.put(hash, bytes)
        _          <- faulty.breakReads
        degraded   <- BodyStore.read(hash).provideEnvironment(env)
        _          <- faulty.healReads
        healed     <- BodyStore.read(hash).provideEnvironment(env)
      } yield assertTrue(
        absent == BodyRead.Missing,
        degraded == BodyRead.Unavailable,
        healed.toOption.exists(_.sameElements(bytes))
      )
    },
    test("putOrSkip returns false on a store error and true once healed") {
      for {
        (store, _) <- freshStore
        faulty     <- FaultyBodyStore.make(store)
        _          <- faulty.breakWrites
        skipped    <- BodyStore.putOrSkip(hash, bytes, source).provideEnvironment(ZEnvironment[BodyStore](faulty))
        absent     <- store.get(hash)
        _          <- faulty.healWrites
        stored     <- BodyStore.putOrSkip(hash, bytes, source).provideEnvironment(ZEnvironment[BodyStore](faulty))
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
        readExit   <- BodyStore.read(hash).provideEnvironment(env).exit
        writeExit  <- BodyStore.putOrSkip(hash, bytes, source).provideEnvironment(env).exit
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

  /** #211: the store must be bounded in '''time''', not only in failure — a slow store is the more likely R2 failure
    * mode, and an unbounded wait makes its latency ours on every fetch.
    *
    * Live clock throughout: [[FaultyBodyStore]]'s stalls are real blocking sleeps precisely so they don't honour
    * `Thread.interrupt`, and a `TestClock` cannot advance past one.
    */
  private def suiteDeadlines = suite("deadlines")(
    test("a stalled read returns at the deadline rather than when the store finishes") {
      // The assertion that catches a dropped `.disconnect`. Without it, `timeoutFail` waits for the inner effect's
      // interruption to complete — which an interrupt-deaf blocking call never does — so the elapsed time would be
      // the full stall rather than the budget.
      // Budgets are deliberately asymmetric: equal ones would let a read/write swap in `Deadlines` pass unnoticed.
      // Asserting the breached `limit` is what kills that mutation; asserting `op` pins the label the operator sees
      // in the DEBUG line.
      val limits = BodyStore.BodyStoreLimits(read = 200.millis, write = 800.millis)
      val breach = (exit: Exit[Throwable, Any]) =>
        exit.causeOption.flatMap(_.failureOption).collect { case t: BodyStore.BodyStoreTimeoutException => t }
      for {
        (store, _)      <- freshStore
        faulty          <- FaultyBodyStore.make(store)
        _               <- faulty.stallReads(5.seconds)
        _               <- faulty.stallWrites(5.seconds)
        bounded          = new BodyStore.Deadlines(faulty, limits)
        (elapsed, exit) <- bounded.get(hash).exit.timed
        putExit         <- bounded.put(hash, bytes).exit
        deleteExit      <- bounded.delete(hash).exit
      } yield assertTrue(
        exit.isFailure,
        elapsed.toMillis < 3000,
        breach(exit).exists(t => t.op == "read" && t.limit.toMillis == 200L),
        breach(putExit).exists(t => t.op == "write" && t.limit.toMillis == 800L),
        // Deletes ride the write budget on purpose: an outage takes the whole store down, not one verb.
        breach(deleteExit).exists(t => t.op == "delete" && t.limit.toMillis == 800L),
        // The size is asserted as a field, not only through the rendered message: it exists so a later latency
        // breaker can match on it, and a message reformat must not silently drop that contract.
        breach(putExit).exists(_.bytes.contains(bytes.length)),
        breach(exit).exists(_.bytes.isEmpty),
        breach(deleteExit).exists(_.bytes.isEmpty)
      )
    },
    test("a breached write names the object's size; a read and a delete have none to name") {
      // #222: `HealthLogging` prints only the throwable's message, so the size must travel on the exception to
      // reach the one line an operator reads.
      val limits = BodyStore.BodyStoreLimits(read = 150.millis, write = 150.millis)
      for {
        (store, _) <- freshStore
        faulty     <- FaultyBodyStore.make(store)
        _          <- faulty.stallReads(5.seconds)
        _          <- faulty.stallWrites(5.seconds)
        bounded     = new BodyStore.Deadlines(faulty, limits)
        putExit    <- bounded.put(hash, bytes).exit
        readExit   <- bounded.get(hash).exit
        deleteExit <- bounded.delete(hash).exit
        // `Option.contains` below is whole-string equality, not substring matching — the assertions pin the entire
        // rendered message, which is what the operator reads.
        exactMessage = (exit: Exit[Throwable, Any]) => exit.causeOption.flatMap(_.failureOption).map(_.getMessage)
      } yield assertTrue(
        exactMessage(putExit).contains(s"BodyStore write of ${bytes.length} bytes exceeded its 150ms deadline"),
        exactMessage(readExit).contains("BodyStore read exceeded its 150ms deadline"),
        exactMessage(deleteExit).contains("BodyStore delete exceeded its 150ms deadline")
      )
    },
    test("the size reaches the operator's WARN, not just the exception") {
      // The test above pins the message; this pins that it survives `safeMessage` into the WARN itself.
      val limits = BodyStore.BodyStoreLimits(read = 150.millis, write = 150.millis)
      capturingLogs { sink =>
        for {
          (store, _) <- freshStore
          faulty     <- FaultyBodyStore.make(store)
          degraded   <- Ref.make(false)
          _          <- faulty.stallWrites(5.seconds)
          monitored   = new BodyStore.HealthLogging(new BodyStore.Deadlines(faulty, limits), degraded)
          _          <- monitored.put(hash, bytes).ignore
          entries    <- sink
          warnings    = entries.collect { case (LogLevel.Warning, msg) => msg }
        } yield assertTrue(warnings.exists(_.contains(s"write of ${bytes.length} bytes")))
      }
    },
    test("a stalled store degrades exactly as a broken one: Unavailable on read, skipped write") {
      // The whole point of raising a typed failure rather than a new outcome: the accessors' existing `catchAll`
      // already folds it, so a stall needs no new degradation semantics anywhere upstream.
      val limits = BodyStore.BodyStoreLimits(read = 150.millis, write = 150.millis)
      for {
        (store, _) <- freshStore
        faulty     <- FaultyBodyStore.make(store)
        _          <- faulty.stallReads(5.seconds)
        _          <- faulty.stallWrites(5.seconds)
        env         = ZEnvironment[BodyStore](new BodyStore.Deadlines(faulty, limits))
        read       <- BodyStore.read(hash).provideEnvironment(env)
        wrote      <- BodyStore.putOrSkip(hash, bytes, source).provideEnvironment(env)
        stored     <- store.get(hash)
      } yield assertTrue(read == BodyRead.Unavailable, !wrote, stored.isEmpty)
    },
    test("a stall trips the degraded flag only when the deadline nests inside the health decorator") {
      // Decorator order is invisible at the callsite and nothing else in the suite would catch an inversion.
      // `HealthLogging.track` observes with `tapBoth`, which does not fire on interruption, and `Deadlines`
      // interrupts the inner effect — so with the order flipped, a store slow enough to blow every deadline never
      // announces itself and the operator loses the one WARN saying the cache is off.
      val limits = BodyStore.BodyStoreLimits(read = 150.millis, write = 150.millis)
      capturingLogs { sink =>
        for {
          (store, _)  <- freshStore
          faulty      <- FaultyBodyStore.make(store)
          _           <- faulty.stallReads(3.seconds)
          insideFlag  <- Ref.make(false)
          outsideFlag <- Ref.make(false)
          inside       = new BodyStore.HealthLogging(new BodyStore.Deadlines(faulty, limits), insideFlag)
          outside      = new BodyStore.Deadlines(new BodyStore.HealthLogging(faulty, outsideFlag), limits)
          _           <- inside.get(hash).ignore
          _           <- outside.get(hash).ignore
          insideSaw   <- insideFlag.get
          outsideSaw  <- outsideFlag.get
          entries     <- sink
          warnings     = entries.count((level, msg) => level == LogLevel.Warning && msg.contains("BodyStore unavailable"))
        } yield assertTrue(insideSaw, !outsideSaw, warnings == 1)
      }
    },
    test("delete is bounded too, so a hung store cannot stall the retention sweep") {
      // `Tables.retentionSweep` drives `BodyStore.delete` once per swept hash, so an unbounded delete parks the
      // retention fiber until the next restart rather than for one pass.
      val limits = BodyStore.BodyStoreLimits(read = 150.millis, write = 150.millis)
      for {
        (store, _)      <- freshStore
        faulty          <- FaultyBodyStore.make(store)
        _               <- faulty.stallWrites(5.seconds)
        bounded          = new BodyStore.Deadlines(faulty, limits)
        (elapsed, exit) <- bounded.delete(hash).exit.timed
      } yield assertTrue(exit.isFailure, elapsed.toMillis < 3000)
    },
    test("live builds the pinned nesting, with HealthLogging outermost") {
      // The test above pins what the two orders *do*; this pins which one `live` picks, which is otherwise
      // unobservable — the decorators are structural, and a stalling store cannot be injected under `live`.
      ZIO.scoped {
        BodyStore.live.build.map(env => assertTrue(env.get[BodyStore].isInstanceOf[BodyStore.HealthLogging]))
      }
    },
    test("an outer interruption still propagates through a bounded operation") {
      // `.disconnect` must not cost us shutdown: a fiber parked on a stalled store has to die when the scope does.
      val limits = BodyStore.BodyStoreLimits(read = 30.seconds, write = 30.seconds)
      for {
        (store, _)         <- freshStore
        faulty             <- FaultyBodyStore.make(store)
        _                  <- faulty.stallReads(30.seconds)
        bounded             = new BodyStore.Deadlines(faulty, limits)
        fiber              <- bounded.get(hash).fork
        _                  <- ZIO.sleep(50.millis)
        (elapsed, exit)    <- fiber.interrupt.timed
      } yield assertTrue(exit.isInterrupted, elapsed.toMillis < 3000)
    }
  ) @@ TestAspect.withLiveClock

  /** #211 direction (4): the transport needs a ceiling of its own. `.disconnect` frees the *caller* at the deadline,
    * but an interrupt-deaf socket read runs on regardless — without an SDK timeout that orphan is a leaked
    * blocking-pool thread per read, so the two halves ship together.
    */
  private def suiteTimeoutConfig = suite("timeout config")(
    test("absent keys fall back to the compiled defaults") {
      val resolved = BodyStore.limitsFrom(storeConfig(None, None, None, None))
      assertTrue(
        resolved.map(_.read.toMillis) == Right(BodyStore.DefaultReadTimeoutMs.toLong),
        resolved.map(_.write.toMillis) == Right(BodyStore.DefaultWriteTimeoutMs.toLong)
      )
    },
    test("explicit values win, and a non-positive one fails the layer with an actionable message") {
      val explicit = BodyStore.limitsFrom(storeConfig(Some(1500), Some(2500), None, None))
      val zero     = BodyStore.limitsFrom(storeConfig(Some(0), None, None, None))
      val negative = BodyStore.limitsFrom(storeConfig(None, Some(-1), None, None))
      assertTrue(
        explicit.map(_.read.toMillis) == Right(1500L),
        explicit.map(_.write.toMillis) == Right(2500L),
        zero.left.exists(_.contains("body-store.read-timeout-ms")),
        negative.left.exists(_.contains("body-store.write-timeout-ms"))
      )
    },
    test("the SDK budget is derived from the accessor deadlines, never configured independently") {
      val limits  = BodyStore.BodyStoreLimits(read = 3.seconds, write = 9.seconds)
      val derived = BodyStore.s3Timeouts(storeConfig(None, None, None, None), limits)
      assertTrue(
        derived.map(_.connect.toMillis) == Right(BodyStore.DefaultS3ConnectTimeoutMs.toLong),
        derived.map(_.socket.toMillis) == Right(BodyStore.DefaultS3SocketTimeoutMs.toLong),
        derived.map(_.apiCall.toMillis) == Right(9000L),
        // The widest deadline, not the narrower one: an abandoned read is bounded by the write budget on purpose.
        derived.map(_.apiCallAttempt.toMillis) == Right(9000L)
      )
    },
    test("the budget follows the widest deadline even when that is the read one") {
      // Both derived values now come from `widest`, so a mutation to `limits.write` would survive every other case
      // in this suite — all of which happen to have write >= read.
      val readWidest = BodyStore.BodyStoreLimits(read = 9.seconds, write = 3.seconds)
      val derived    = BodyStore.s3Timeouts(storeConfig(None, None, None, None), readWidest)
      assertTrue(
        derived.map(_.apiCall.toMillis) == Right(9000L),
        derived.map(_.apiCallAttempt.toMillis) == Right(9000L)
      )
    },
    test("one attempt may spend the whole SDK budget, so a slow upload is never restarted from byte zero") {
      // #222: `apiCallAttempt` used to track the socket timeout, and `ApiCallAttemptTimeoutException` is retryable,
      // so a put slower than it was cut, restarted from byte zero, and cut again — never able to succeed. Asserted
      // as a relationship, since the mutation to catch is "derive the attempt budget from something narrower
      // again", which two literals updated in lockstep would miss.
      val defaults = BodyStore.BodyStoreLimits(
        read = BodyStore.DefaultReadTimeoutMs.millis,
        write = BodyStore.DefaultWriteTimeoutMs.millis
      )
      val widerSocket = BodyStore.s3Timeouts(storeConfig(None, None, None, Some(9000)), defaults)
      val derived     = BodyStore.s3Timeouts(storeConfig(None, None, None, None), defaults)
      assertTrue(
        derived.map(t => t.apiCallAttempt == t.apiCall) == Right(true),
        // The socket timeout must no longer bound an attempt: at the shipped defaults it is strictly smaller...
        derived.map(t => t.socket.compareTo(t.apiCallAttempt) < 0) == Right(true),
        // ...and moving it must not move the attempt budget with it.
        widerSocket.map(_.apiCallAttempt) == derived.map(_.apiCallAttempt)
      )
    },
    test("the accessor deadline is what the operator sees breached, so the SDK must not give up first") {
      // Deliberately NOT shortened for "diagnosis": that surfaces only `ApiCallTimeoutException`, which carries no
      // status code, in place of `BodyStoreTimeoutException`, which names the operation, budget and size. See
      // `BodyStore.S3Timeouts`.
      val limits  = BodyStore.BodyStoreLimits(read = 5.seconds, write = 10.seconds)
      val derived = BodyStore.s3Timeouts(storeConfig(None, None, None, None), limits)
      assertTrue(derived.map(_.apiCall) == Right(limits.write))
    },
    test("a socket timeout wider than the widest accessor deadline is rejected") {
      val limits = BodyStore.BodyStoreLimits(read = 1.second, write = 2.seconds)
      val bad    = BodyStore.s3Timeouts(storeConfig(None, None, None, Some(30000)), limits)
      assertTrue(bad.left.exists(_.contains("must not exceed the widest accessor deadline")))
    },
    test("the derived durations actually reach the SDK's override configuration") {
      // The defect being fixed is literally "the builder was called bare", and the SDK does not expose its resolved
      // configuration off a built S3Client — so assert on the extracted, pure builder input instead.
      val timeouts = BodyStore.S3Timeouts(
        connect = 1.second,
        socket = 4.seconds,
        apiCallAttempt = 4.seconds,
        apiCall = 8.seconds
      )
      val overrides = BodyStore.s3Overrides(timeouts)
      assertTrue(
        overrides.apiCallTimeout.isPresent,
        overrides.apiCallTimeout.get == 8.seconds,
        overrides.apiCallAttemptTimeout.isPresent,
        overrides.apiCallAttemptTimeout.get == 4.seconds
      )
    },
    test("the derived durations actually reach the built S3 client") {
      // The mapping test above would still pass if `buildS3Client` dropped the `.overrideConfiguration` call —
      // which is precisely the defect being fixed ("the builder was called bare"). Read the applied configuration
      // back off a built client instead. No connection is opened and no credential chain is consulted: region,
      // credentials and endpoint are all explicit.
      val timeouts = BodyStore.S3Timeouts(
        connect = 1.second,
        socket = 4.seconds,
        apiCallAttempt = 4.seconds,
        apiCall = 8.seconds
      )
      ZIO.acquireReleaseWith(
        ZIO.attempt(
          BodyStore.buildS3Client(
            endpoint = "https://example.invalid",
            region = "auto",
            accessKey = "test-access-key",
            secretKey = "test-secret-key",
            timeouts = timeouts
          )
        )
      )(client => ZIO.attempt(client.close()).ignore) { client =>
        val applied = client.serviceClientConfiguration.overrideConfiguration
        ZIO.succeed(
          assertTrue(
            applied.apiCallTimeout.isPresent,
            applied.apiCallTimeout.get == 8.seconds,
            applied.apiCallAttemptTimeout.isPresent,
            applied.apiCallAttemptTimeout.get == 4.seconds
          )
        )
      }
    }
  )

}
