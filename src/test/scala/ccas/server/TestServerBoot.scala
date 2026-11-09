package ccas.server

import java.io.{ByteArrayOutputStream, PrintStream}
import java.net.{InetAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.sql.{Connection, SQLException}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.postgresql.ds.PGSimpleDataSource
import zio.{
  durationInt,
  Chunk,
  Duration,
  Exit,
  Fiber,
  Promise,
  Ref,
  RIO,
  Scope,
  Task,
  UIO,
  URIO,
  ZEnvironment,
  ZIO,
  ZLayer
}
import zio.http.{Client, Server}
import zio.stream.ZStream
import zio.test.{assertTrue, Live, Spec, TestAspect, TestClock, ZIOSpecDefault, ZTestLogger}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.cli.CcasApiClient
import ccas.server.jobs.{JobEffect, JobKind, JobLogs, JobRun, JobRunner}
import ccas.server.routes.JobRoutes.JobStatusResponse
import ccas.server.scheduler.JobScheduler
import ccas.utils.{JobLogSink, ProgressDisplay}
import ccas.utils.client.{BodyStore, FsBodyStore, TestChessComClientSupport}
import ccas.utils.sql.{DatabaseAddress, PostgresClient}

/** What `CcasServer` does at boot: the pid file a detached server owns, what it says and asks about its database
  * (#320), the order it shuts down in and what it logs as it does (#316), and the logger its services and HTTP server
  * inherit (#334, #337). The boot line's client never connects: the line has to come out before the first connection,
  * so a boot that fails to connect still names where it tried. The terminal conversation itself is not reachable from
  * a test; [[BootGuard]]'s decisions are.
  */
object TestServerBoot extends ZIOSpecDefault {

  private val local  = Some(DatabaseAddress("localhost:5432", "/ccas"))
  private val remote = Some(DatabaseAddress("ep-x.neon.tech", "/neondb"))

  private val unreachablePool = PostgresClient.live(prefix = "databaseUnreachable", onInit = CcasServer.poolInit)

  private val unreachableBoot = ZIO.scoped(BootLog.layer("the database pool")(unreachablePool).build).exit

  private val loggedMessages = ZTestLogger.logOutput.map(_.map(_.message()))

  private def bootLines(log: ByteArrayOutputStream): UIO[Vector[String]] =
    ZIO.succeed(JobLogSink.stripAnsi(log.toString(StandardCharsets.UTF_8)).linesIterator.toVector.collect {
      case BootLine(line) => line
    })

  private def bootHasStarted(log: ByteArrayOutputStream): UIO[Vector[String]] =
    bootLines(log).repeatUntil(_.lastOption.exists(_.startsWith("Started in ")))

  private def stillStartingNotices(log: ByteArrayOutputStream): UIO[Vector[String]] =
    bootLines(log).map(_.filter(_.startsWith("Still starting")))

  // Waits for the notice fibre to be asleep, then for its line: an `adjust` landing before it sleeps would only push its
  // wake-up back.
  private def thirtySecondsPass(log: ByteArrayOutputStream, linesBy: Int): UIO[Vector[String]] =
    TestClock.sleeps.repeatUntil(_.nonEmpty) *> TestClock.adjust(30.seconds) *>
      stillStartingNotices(log).repeatUntil(_.size == linesBy)

  // Real time on both sides of the `adjust`, for a notice fibre that outlived the boot to reach its sleep and then log:
  // no line can be waited for here, because the guarantee is that none comes.
  private def noticesAMinuteLater(log: ByteArrayOutputStream): UIO[Vector[String]] =
    Live.live(ZIO.sleep(50.millis)) *> TestClock.adjust(1.minute) *> Live.live(ZIO.sleep(50.millis)) *>
      stillStartingNotices(log)

  override def spec: Spec[Any, Throwable] = suite("TestServerBoot")(
    test("a detached server writes its own pid to the pid file, and deletes the file on shutdown") {
      for {
        dir <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-pid"))
        path = dir.resolve("state").resolve("ccas-8080.pid")
        during <- ZIO.scoped(CcasServer.pidFileManaged(Some(path)) *> ZIO.attemptBlocking(Files.readString(path)))
        after  <- ZIO.attemptBlocking(Files.exists(path))
      } yield assertTrue(during == s"${ProcessHandle.current().pid()}\n", !after)
    },
    test(
      "shutdown stops the loops, then the running jobs, then the HTTP server, before the job runner and the pool are " +
        "released"
    ) {
      for {
        (exit, events) <- serveThenInterrupt(pidFileOption = None)
      } yield assertTrue(
        exit.isInterruptedOnly,
        events.take(2).toSet == Set("scheduler loop interrupted", "retention sweep interrupted"),
        events.slice(2, 4) == Vector("jobs interrupted", "server stopped"),
        events.drop(4).toSet == Set("job runner released", "pool closed")
      )
    },
    test("shutdown logs that it has begun, each step in the order it stops them, and when it has stopped") {
      for {
        booted <- boot(pidFileOption = None)
        exit   <- booted.fiber.interrupt
        lines = booted.shutdownLines
      } yield assertTrue(
        exit.isInterruptedOnly,
        lines.headOption.contains("Shutting down"),
        lines.filter(_.startsWith("Stopping ")) == Vector(
          "Stopping the retention sweep",
          "Stopping the scheduler",
          "Stopping the running jobs",
          "Stopping the HTTP server"
        ),
        lines.filter(_.startsWith("Stopped the ")).map(_.replaceFirst(""" in \d+ ms$""", "")) == Vector(
          "Stopped the retention sweep",
          "Stopped the scheduler",
          "Stopped the running jobs",
          "Stopped the HTTP server"
        ),
        lines.lastOption.exists(_.startsWith("Stopped in "))
      )
    },
    test("the HTTP server stops without waiting out Netty's default 2 s quiet period") {
      val HttpServerStopped = """Stopped the HTTP server in (\d+) ms""".r
      for {
        booted <- boot(pidFileOption = None)
        _      <- booted.fiber.interrupt
        stopMillisOption = booted.shutdownLines.collectFirst { case HttpServerStopped(millis) => millis.toLong }
      } yield assertTrue(stopMillisOption.exists(_ < 2000))
    } @@ TestAspect.withLiveClock,
    test("a layer's release is logged as a step, around the release the layer itself registered") {
      val pool = ZLayer.scoped(ZIO.acquireRelease(ZIO.unit)(_ => ZIO.logInfo("pool closed")))
      for {
        _    <- ZIO.scoped(ShutdownLog.layer("the pool")(pool).build)
        logs <- ZTestLogger.logOutput
      } yield assertTrue(logs.map(_.message()) == Chunk("Stopping the pool", "pool closed", "Stopped the pool in 0 ms"))
    }.provide(ZTestLogger.default),
    test("a step's release is logged as it starts and as it ends, with how long it took") {
      for {
        closing <- ZIO.scoped(ShutdownLog.step("the pool")(ZIO.addFinalizer(ZIO.sleep(2.seconds)))).fork
        _       <- TestClock.adjust(2.seconds)
        _       <- closing.join
        logs    <- ZTestLogger.logOutput
      } yield assertTrue(logs.map(_.message()) == Chunk("Stopping the pool", "Stopped the pool in 2000 ms"))
    }.provide(ZTestLogger.default),
    test("a step whose release hasn't ended has logged that it is stopping but not that it stopped") {
      for {
        releasing <- Promise.make[Nothing, Unit]
        released  <- Promise.make[Nothing, Unit]
        release = releasing.succeed(()) *> released.await
        closing <- ZIO.scoped(ShutdownLog.step("the pool")(ZIO.addFinalizer(release))).fork
        _       <- releasing.await
        during  <- ZTestLogger.logOutput
        _       <- released.succeed(())
        _       <- closing.join
      } yield assertTrue(during.map(_.message()) == Chunk("Stopping the pool"))
    }.provide(ZTestLogger.default),
    test("a detached server's pid file outlives the services it owns") {
      for {
        dir <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-pid"))
        path = dir.resolve("ccas-8080.pid")
        (exit, events) <- serveThenInterrupt(pidFileOption = Some(path))
        after          <- ZIO.attemptBlocking(Files.exists(path))
      } yield assertTrue(exit.isInterruptedOnly, events.contains("pid file present at pool close"), !after)
    },
    test("a client following a running job doesn't hold shutdown for the HTTP server's drain") {
      for {
        booted    <- boot(pidFileOption = None)
        api       <- CcasApiClient.live(s"http://127.0.0.1:${booted.port}")
        following <- Promise.make[Nothing, Unit]
        follow    <- api.streamLines("/api/jobs/job-1/logs")(_ => following.succeed(()).unit).fork
        _         <- following.await.raceFirst(follow.join *> ZIO.never)
        // zio-http's drain gives up after 10 s, so a follow that held it would take at least that.
        (elapsed, exit) <- booted.fiber.interrupt.timed
        followed        <- follow.join.exit
      } yield assertTrue(exit.isInterruptedOnly, followed.isSuccess, elapsed.toMillis < 5000)
    }.provide(Client.default) @@ TestAspect.withLiveClock,
    // So the server's log keeps one format, and zio-http's warning for a read-idle reap meets ADR 0015's filter.
    test("the HTTP server logs through the server's display, as the loops and jobs do") {
      for {
        booted <- boot(pidFileOption = None)
        api    <- CcasApiClient.live(s"http://127.0.0.1:${booted.port}")
        _      <- api.getJson[JobStatusResponse]("/api/jobs/job-1").exit
        _      <- booted.fiber.interrupt
      } yield assertTrue(booted.logged.contains("status of job-1 requested"))
    }.provide(Client.default) @@ TestAspect.withLiveClock,
    test("a boot names its database before it connects, so a boot that can't connect still says where it tried") {
      for {
        exit <- unreachableBoot
        logs <- ZTestLogger.logOutput
        messages = logs.map(_.message())
      } yield assertTrue(
        exit.isFailure,
        messages.contains("Database: 127.0.0.1:1/nowhere"),
        !messages.exists(_.contains("alice")),
        !messages.exists(_.contains("hunter2"))
      )
    }.provide(ZTestLogger.default),
    // The timeout makes a regression fail rather than hang: these wait on a notice fibre or a blocked thread.
    suite("boot log")(
      test("a step logs when it starts and when it has started, with how long it took") {
        for {
          building <- BootLog.step("the pool")(ZIO.sleep(2.seconds)).fork
          _        <- TestClock.sleeps.repeatUntil(_.nonEmpty)
          _        <- TestClock.adjust(2.seconds)
          _        <- building.join
          messages <- loggedMessages
        } yield assertTrue(messages == Chunk("Starting the pool", "Started the pool in 2 s"))
      },
      test("a step still building has logged that it is starting but not that it has started") {
        for {
          finish   <- Promise.make[Nothing, Unit]
          building <- BootLog.step("the pool")(finish.await).fork
          during   <- loggedMessages.repeatUntil(_.nonEmpty)
          _        <- finish.succeed(())
          _        <- building.join
        } yield assertTrue(during == Chunk("Starting the pool"))
      },
      test("a step that fails logs no started line, so the failed step is the one left open") {
        for {
          exit     <- BootLog.step("the pool")(ZIO.fail("no connection")).exit
          messages <- loggedMessages
        } yield assertTrue(exit == Exit.fail("no connection"), messages == Chunk("Starting the pool"))
      },
      test("a layer logs its start only once the layers it needs are built") {
        for {
          finish <- Promise.make[Nothing, Unit]
          pool   = BootLog.layer("the pool")(ZLayer.fromZIO(finish.await.as(1)))
          runner = BootLog.layer("the job runner")(ZLayer.fromFunction((pool: Int) => pool.toString))
          building <- ZIO.scoped((pool >>> runner).build).fork
          _        <- loggedMessages.repeatUntil(_.nonEmpty)
          // No line can be waited for here, because the guarantee is that none comes yet.
          _      <- Live.live(ZIO.sleep(50.millis))
          during <- loggedMessages
          _      <- finish.succeed(())
          _      <- building.join
          after  <- loggedMessages
        } yield assertTrue(
          during == Chunk("Starting the pool"),
          after == Chunk(
            "Starting the pool",
            "Started the pool in 0 ms",
            "Starting the job runner",
            "Started the job runner in 0 ms"
          )
        )
      },
      test("a step's time reads in milliseconds under a second and in minutes and seconds above") {
        assertTrue(
          BootLog.renderTime(Duration.fromNanos(400_000L)) == "0 ms",
          BootLog.renderTime(999.millis) == "999 ms",
          BootLog.renderTime(1.second) == "1 s",
          BootLog.renderTime(252_125.millis) == "4 m 12 s"
        )
      },
      test("a boot logs that it has begun, each step it builds, and when it has started") {
        for {
          booted <- boot(pidFileOption = None)
          lines  <- bootHasStarted(booted.log)
          _      <- booted.fiber.interrupt
        } yield assertTrue(
          lines == Vector(
            "Starting up",
            "Starting the HTTP server",
            "Started the HTTP server in 0 ms",
            "Started in 0 ms"
          )
        )
      },
      // zio-http binds on the first install, so a boot that says it has started before then is wrong about this case.
      test("a boot whose port is taken leaves the HTTP server step open and never says it has started") {
        ZIO.scoped {
          for {
            taken     <- ZIO.fromAutoCloseable(ZIO.attempt(new ServerSocket(0, 1, InetAddress.getLoopbackAddress)))
            log       <- ZIO.succeed(new ByteArrayOutputStream())
            events    <- Ref.make(Vector.empty[String])
            scheduler <- Promise.make[Nothing, Unit]
            sweep     <- Promise.make[Nothing, Unit]
            jobs      <- Promise.make[Nothing, Unit]
            services = stubServices(events, scheduler, sweep, jobs, pidFileOption = None)
            server   = CcasServer.httpServer(host = "127.0.0.1", port = taken.getLocalPort)
            exit  <- JobLogSink.currentSink.locally(capturing(log))(serving(None, services, server).exit)
            lines <- bootLines(log)
          } yield assertTrue(
            exit.isFailure,
            lines.contains("Starting the HTTP server"),
            !lines.exists(_.startsWith("Started the HTTP server")),
            !lines.exists(_.startsWith("Started in "))
          )
        }
      },
      test("a slow boot keeps saying it is still starting, with how long it has waited") {
        for {
          (fiber, log) <- bootStalledOn(ZIO.never)
          _            <- thirtySecondsPass(log, linesBy = 1)
          _            <- thirtySecondsPass(log, linesBy = 2)
          notices      <- thirtySecondsPass(log, linesBy = 3)
          _            <- fiber.interrupt
        } yield assertTrue(
          notices == Vector(
            "Still starting after 30 s",
            "Still starting after 1 m",
            "Still starting after 1 m 30 s"
          )
        )
      },
      test("a boot that finishes before the first interval logs no still-starting line") {
        for {
          booted  <- boot(pidFileOption = None)
          _       <- bootHasStarted(booted.log)
          notices <- noticesAMinuteLater(booted.log)
          _       <- booted.fiber.interrupt
        } yield assertTrue(notices.isEmpty)
      },
      test("still-starting lines stop once the boot has started") {
        for {
          log    <- ZIO.succeed(new ByteArrayOutputStream())
          finish <- Promise.make[Nothing, Unit]
          // The boot stays on this fiber: a fiber it was forked from would take the server with it on ending.
          firstPass <- (thirtySecondsPass(log, linesBy = 1) <* finish.succeed(())).fork
          booted    <- bootLoggingTo(log, pidFileOption = None, before = finish.await)
          before    <- firstPass.join
          _         <- bootHasStarted(log)
          after     <- noticesAMinuteLater(log)
          _         <- booted.fiber.interrupt
        } yield assertTrue(after == before)
      },
      test("a boot that fails stops the still-starting lines and logs no started line") {
        val failure = new IllegalStateException("no connection")
        for {
          finish       <- Promise.make[Throwable, ZEnvironment[CcasServer.Served]]
          (fiber, log) <- bootStalledOn(finish.await)
          before       <- thirtySecondsPass(log, linesBy = 1)
          _            <- finish.fail(failure)
          exit         <- fiber.await
          after        <- noticesAMinuteLater(log)
          lines        <- bootLines(log)
        } yield assertTrue(exit == Exit.fail(failure), after == before, !lines.exists(_.startsWith("Started in ")))
      },
      test("a boot interrupted part-way stops the still-starting lines") {
        for {
          (fiber, log) <- bootStalledOn(ZIO.never)
          before       <- thirtySecondsPass(log, linesBy = 1)
          exit         <- fiber.interrupt
          after        <- noticesAMinuteLater(log)
        } yield assertTrue(exit.isInterruptedOnly, after == before)
      },
      // The real clock and a short interval, since the wait happens on an OS thread, which a frozen TestClock can't see
      // past, as in `TestPostgresClientInterrupt`.
      test("a boot stuck on its first connection keeps saying it is still starting") {
        val entered = new CountDownLatch(1)
        val client  = PostgresClient.fromDataSource(new StuckDataSource(entered))
        for {
          fiber <- BootLog.bootEvery(10.millis)(_ => CcasServer.poolInit.provideEnvironment(ZEnvironment(client))).fork
          reached <- ZIO.attemptBlocking(entered.await(10, TimeUnit.SECONDS))
          _       <- ZIO.whenDiscard(!reached)(ZIO.dieMessage("getConnection was never reached"))
          notices <- loggedMessages.map(_.filter(_.startsWith("Still starting"))).delay(10.millis).repeatUntil(
            _.size >= 2
          )
          exit <- fiber.interrupt
        } yield assertTrue(notices.headOption.contains("Still starting after 10 ms"), exit.isInterruptedOnly)
      } @@ TestAspect.withLiveClock,
      test(
        "a boot says it is starting the database pool before its first connection, so a boot that can't connect still " +
          "says so"
      ) {
        for {
          exit     <- unreachableBoot
          messages <- loggedMessages
        } yield assertTrue(
          exit.isFailure,
          messages.contains("Starting the database pool"),
          !messages.exists(_.startsWith("Started the database pool"))
        )
      }
    ).provide(ZTestLogger.default) @@ TestAspect.timeout(20.seconds),
    // Only the pool is real, and its init fails the boot once it has named the database, so neither the other services
    // nor the HTTP server are ever built.
    test("a boot names its database in the server's format, as it logs every line after it") {
      val rest = ZLayer.fail(new IllegalStateException("not reached: the pool fails first"))
      for {
        log <- ZIO.succeed(new ByteArrayOutputStream())
        exit <- JobLogSink.currentSink.locally(capturing(log)) {
          serving(pidFileOption = None, services = unreachablePool >>> rest, server = unbuiltServer).exit
        }
        lines = JobLogSink.stripAnsi(log.toString(StandardCharsets.UTF_8)).linesIterator.toList
      } yield assertTrue(
        exit.isFailure,
        lines.exists(_.matches("""\[INFO \d{2}:\d{2}:\d{2}\] Database: 127\.0\.0\.1:1/nowhere"""))
      )
    },
    // A directory at the file's path, which no reader can read, root included.
    test("an unreadable ccas.env is logged once, in the server's format, before anything the server logs after it") {
      for {
        dir <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-env"))
        file = dir.resolve("ccas.env")
        // Deleted on exit in reverse order, the child first, so the parent is empty by its turn.
        _ <- ZIO.attemptBlocking {
          dir.toFile.deleteOnExit()
          Files.createDirectory(file).toFile.deleteOnExit()
        }
        log <- ZIO.succeed(new ByteArrayOutputStream())
        _ <- JobLogSink.currentSink.locally(capturing(log)) {
          CcasServer.withServerEnv(Right(file))(ZIO.logInfo("Starting the server..."))
        }
        lines = JobLogSink.stripAnsi(log.toString(StandardCharsets.UTF_8)).linesIterator.toList
      } yield assertTrue(
        lines.size == 2,
        lines.head.matches("""\[WARN \d{2}:\d{2}:\d{2}\] could not read server config .+"""),
        lines.head.contains(file.toString),
        lines(1).matches("""\[INFO \d{2}:\d{2}:\d{2}\] Starting the server\.\.\.""")
      )
    },
    test(
      "with nowhere to look for ccas.env, the server warns once, in its format, and carries on from its environment"
    ) {
      for {
        log <- ZIO.succeed(new ByteArrayOutputStream())
        _ <- JobLogSink.currentSink.locally(capturing(log)) {
          CcasServer.withServerEnv(Left("no directory for ccas.env"))(ZIO.logInfo("Starting the server..."))
        }
        lines = JobLogSink.stripAnsi(log.toString(StandardCharsets.UTF_8)).linesIterator.toList
      } yield assertTrue(
        lines.size == 2,
        lines.head.matches("""\[WARN \d{2}:\d{2}:\d{2}\] no directory for ccas\.env"""),
        lines(1).matches("""\[INFO \d{2}:\d{2}:\d{2}\] Starting the server\.\.\.""")
      )
    },
    suite("BootGuard")(
      test("a loopback database boots without asking") {
        assertTrue(BootGuard.question(resolved = Right(local), detachedChild = false).isEmpty)
      },
      test("a remote database asks before booting, naming it") {
        assertTrue(
          BootGuard
            .question(resolved = Right(remote), detachedChild = false)
            .contains("Boot against ep-x.neon.tech/neondb? [y/N] ")
        )
      },
      test("a database whose address can't be read asks before booting") {
        assertTrue(
          BootGuard
            .question(resolved = Right(None), detachedChild = false)
            .contains("Boot against a database whose address can't be read from the configuration? [y/N] ")
        )
      },
      test("a detached child never asks, since its parent did") {
        assertTrue(BootGuard.question(resolved = Right(remote), detachedChild = true).isEmpty)
      },
      test("a config that doesn't resolve boots without asking, so the pool fails with its own message") {
        val unresolved = Left(new IllegalArgumentException("database.url is blank"))
        assertTrue(BootGuard.question(resolved = unresolved, detachedChild = false).isEmpty)
      }
    )
  )

  /** A `CcasServer.serveWith` running in the background over stub services, with both loops started and the HTTP
    * server listening on `port`. `events` is what the stubs have recorded, in order, and `logged` every line the
    * server's display has written outside a job, which logs to its own sink, as the console would show them.
    */
  private final case class Booted(
    fiber: Fiber[Throwable, Unit],
    port: Int,
    events: Ref[Vector[String]],
    log: ByteArrayOutputStream
  ) {
    def logged: String = log.toString(StandardCharsets.UTF_8)

    def shutdownLines: Vector[String] =
      JobLogSink.stripAnsi(logged).linesIterator.toVector.collect { case ShutdownLine(line) => line }
  }

  private def boot(pidFileOption: Option[Path]): Task[Booted] =
    ZIO.succeed(new ByteArrayOutputStream()).flatMap(bootLoggingTo(_, pidFileOption, before = ZIO.unit))

  /** [[boot]], logging to `log`, with `before` holding up the services' build. */
  private def bootLoggingTo(
    log: ByteArrayOutputStream,
    pidFileOption: Option[Path],
    before: UIO[Unit]
  ): Task[Booted] =
    for {
      events           <- Ref.make(Vector.empty[String])
      schedulerStarted <- Promise.make[Nothing, Unit]
      sweepStarted     <- Promise.make[Nothing, Unit]
      jobsInterrupted  <- Promise.make[Nothing, Unit]
      built            <- Promise.make[Nothing, Server]
      services = ZLayer.fromZIO(before).flatMap { _ =>
        stubServices(events, schedulerStarted, sweepStarted, jobsInterrupted, pidFileOption)
      }
      // The display writes each line to the sink the logging fiber carries, and the server's fibers inherit this one.
      fiber <- JobLogSink.currentSink.locally(capturing(log)) {
        serving(pidFileOption, services, markedServer(events, built)).fork
      }
      ready = schedulerStarted.await *> sweepStarted.await *> built.await.flatMap(_.port)
      // Racing the join turns a boot that fails before it is ready into a failure rather than a hang.
      port <- ready.raceFirst(fiber.join *> ZIO.never)
    } yield Booted(fiber, port, events, log)

  /** A boot in the background whose services never finish building until `wait` does, and its log. */
  private def bootStalledOn(
    wait: Task[ZEnvironment[CcasServer.Served]]
  ): UIO[(Fiber[Throwable, Unit], ByteArrayOutputStream)] =
    for {
      log <- ZIO.succeed(new ByteArrayOutputStream())
      fiber <- JobLogSink.currentSink.locally(capturing(log)) {
        serving(pidFileOption = None, services = ZLayer.fromZIOEnvironment(wait), server = unbuiltServer).fork
      }
    } yield (fiber, log)

  // Through the wrapper both entry points use, with a ccas.env path no file can exist at, so it applies nothing.
  private def serving(
    pidFileOption: Option[Path],
    services: ZLayer[ProgressDisplay, Throwable, CcasServer.Served],
    server: ZLayer[Any, Throwable, Server]
  ): Task[Unit] =
    CcasServer.withServerEnv(Right(Paths.get("/dev/null/ccas.env"))) {
      ZIO.scoped[ProgressDisplay](CcasServer.serveWith(pidFileOption, services, server))
    }

  /** A database that never hands over a connection: `getConnection` blocks until the thread is interrupted. */
  private final class StuckDataSource(entered: CountDownLatch) extends PGSimpleDataSource {
    override def getConnection(): Connection = {
      entered.countDown()
      Thread.sleep(Long.MaxValue)
      throw new SQLException("unreachable")
    }
  }

  private def capturing(log: ByteArrayOutputStream): JobLogSink = {
    val out = new PrintStream(log, true, StandardCharsets.UTF_8)
    new JobLogSink { override def writeConsoleSync(line: String): Unit = out.println(line) }
  }

  // A line `ShutdownLog` wrote, as the console shows it, less its level, time and source.
  private val ShutdownLine = """\[INFO \d{2}:\d{2}:\d{2}\] \[shutdown\] (.*)""".r

  // The same for `BootLog`.
  private val BootLine = """\[INFO \d{2}:\d{2}:\d{2}\] \[boot\] (.*)""".r

  // Port 0, so a change that built it after all could never take a real server's port.
  private val unbuiltServer = Server.defaultWith(_.binding("127.0.0.1", 0))

  /** Boots, then interrupts the server as a SIGTERM would. Returns its exit and the events the stubs recorded. */
  private def serveThenInterrupt(
    pidFileOption: Option[Path]
  ): ZIO[Any, Throwable, (Exit[Throwable, Unit], Vector[String])] =
    for {
      booted   <- boot(pidFileOption)
      exit     <- booted.fiber.interrupt
      recorded <- booted.events.get
    } yield (exit, recorded)

  private def stubServices(
    events: Ref[Vector[String]],
    schedulerStarted: Promise[Nothing, Unit],
    sweepStarted: Promise[Nothing, Unit],
    jobsInterrupted: Promise[Nothing, Unit],
    pidFileOption: Option[Path]
  ): ZLayer[Any, Throwable, CcasServer.Served] = {
    def record(event: String): UIO[Unit] = events.update(_ :+ event)
    val pidFileAtPoolClose = ZIO.foreachDiscard(pidFileOption) { path =>
      ZIO.whenZIODiscard(ZIO.attemptBlocking(Files.exists(path)).orDie)(record("pid file present at pool close"))
    }
    ZLayer.make[CcasServer.Served](
      ZLayer.succeed[JobScheduler](new LoopingScheduler(schedulerStarted, record("scheduler loop interrupted"))),
      ZLayer.scoped(
        ZIO.acquireRelease(ZIO.succeed[JobRunner](new StubJobRunner(record, sweepStarted, jobsInterrupted))) { _ =>
          record("job runner released")
        }
      ),
      // A data source that is never asked for a connection, so the pool's release is all the test sees of it.
      ZLayer.scoped(
        ZIO.acquireRelease(ZIO.succeed(PostgresClient.fromDataSource(new PGSimpleDataSource()))) { _ =>
          pidFileAtPoolClose *> record("pool closed")
        }
      ),
      ZLayer.succeed[BodyStore](new FsBodyStore(Paths.get("target/test-bodies"))),
      TestChessComClientSupport.dummyLayer
    )
  }

  // The server `CcasServer` boots, on a free port. The marker is built first, so its finalizer runs once the server's own
  // release has finished.
  private def markedServer(
    events: Ref[Vector[String]],
    built: Promise[Nothing, Server]
  ): ZLayer[Any, Throwable, Server] =
    ZLayer
      .scoped(ZIO.addFinalizer(events.update(_ :+ "server stopped")))
      .flatMap(_ => CcasServer.httpServer(host = "127.0.0.1", port = 0))
      .tap(env => built.succeed(env.get[Server]))

  private final class LoopingScheduler(started: Promise[Nothing, Unit], interrupted: UIO[Unit]) extends JobScheduler {
    override def start: URIO[Scope, Unit] = (started.succeed(()) *> ZIO.never).onInterrupt(interrupted).forkScoped.unit
  }

  /** A job runner with one job running, whose log stream stays open until `interruptAll`, as the real runner's does.
    * Its `sweepLogs`, the first half of the retention pass a boot starts at once, never finishes. Its `status` logs, as
    * a request handler might, then finds no job.
    */
  private final class StubJobRunner(
    record: String => UIO[Unit],
    sweepStarted: Promise[Nothing, Unit],
    jobsInterrupted: Promise[Nothing, Unit]
  ) extends JobRunner {
    private def unused: Nothing = throw new NotImplementedError("TestServerBoot drives a sweep, a follow and shutdown")

    override def sweepLogs: URIO[PostgresClient, Int] =
      (sweepStarted.succeed(()) *> ZIO.never).onInterrupt(record("retention sweep interrupted"))

    override def interruptAll: UIO[Unit] = record("jobs interrupted") *> jobsInterrupted.succeed(()).unit

    override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs] =
      ZIO.succeed(JobLogs.Streaming(ZStream.succeed("started") ++ ZStream.fromZIO(jobsInterrupted.await).drain))

    override def submit(
      kind: JobKind,
      clubIdOption: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: JobEffect
    ): RIO[PostgresClient, JobRunId] = unused

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] =
      ZIO.logInfo(s"status of $id requested").as(None)

    override def cancel(id: JobRunId): UIO[Boolean]                        = unused
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = unused

    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = unused
  }
}
